import type { ParsedPlaylist, PreloadHints } from 'hls-playlist-reader/playlist';

import { M3U8Playlist, MediaPlaylist, MediaSegment, IndependentSegment, AttrList } from 'm3u8parse';

import { HlsIndexMeta, HlsPlaylistFetcher, PlaylistObject } from 'hls-playlist-reader/fetcher';
import { AbortController, assert, Deferred, wait } from 'hls-playlist-reader/helpers';

let setMaxListeners = (_n: number, _target: object) => undefined;
if (typeof process === 'object') {
    setMaxListeners = (await import('node' + ':events')).setMaxListeners;
}

interface AbortSignal<T = any> extends globalThis.AbortSignal {
    readonly reason: T;
}

// eslint-disable-next-line @typescript-eslint/no-redeclare
interface AbortController<T = any> extends globalThis.AbortController {
    readonly signal: AbortSignal<T>;
    abort(reason?: T): void;
}


export enum EvictionReason {
    /** The segment has been evicted in the latest playlist update. */
    Expired = 'expired'
}


class SegmentPointer {

    readonly msn: number;
    readonly part?: number;

    constructor(msn = -1, part?: number) {

        this.msn = +msn;
        this.part = part;
    }

    next(): SegmentPointer {

        return new SegmentPointer(this.msn + 1, this.part === undefined ? undefined : 0);
    }

    isEmpty(): boolean {

        return this.msn < 0;
    }
}

class HintedSegment extends MediaSegment {}

export class HlsFetcherObject {

    readonly msn: number;
    readonly isClosed: boolean;
    readonly offset?: number;
    readonly baseUrl: string;
    hints?: PreloadHints;

    onUpdate?: ((entry: IndependentSegment, old?: IndependentSegment) => void) = undefined;

    private _entry: IndependentSegment;
    #closed?: Deferred<true>;
    #evicted: AbortSignal<EvictionReason | Error>;

    /**
     * @param offset Segment offset in ms since startDate / start of the initial playlist
     */
    constructor(msn: number, segment: IndependentSegment, { offset, baseUrl, signal }: { baseUrl: string; offset?: number; signal: AbortSignal }) {

        this.msn = msn;
        this._entry = new MediaSegment(segment) as IndependentSegment;
        this.isClosed = !segment.isPartial();
        this.offset = offset;
        this.baseUrl = baseUrl;

        this.#evicted = signal;
    }

    get entry(): IndependentSegment {

        return this._entry;
    }

    set entry(entry: IndependentSegment) {

        assert(!this.isClosed);

        const old = this._entry;
        this._entry = Object.assign(new MediaSegment(entry), {
            discontinuity: !!(+entry.discontinuity | +old.discontinuity)
        }) as IndependentSegment;

        this._update(!entry.isPartial(), old);
    }

    /**
     * Resolves once no more parts will be added.
     */
    closed(): PromiseLike<true> | true {

        if (this.isClosed) {
            return true;
        }

        if (!this.#closed) {
            this.#closed = new Deferred<true>();
        }

        return this.#closed.promise;
    }

    close(): void {

        if (!this.isClosed) {
            return this._update(true);
        }
    }

    /**
     * Triggers if the segment data is no longer retrievable.
     *
     * The trigger reason will be `'expired'` to signal that the segment
     * is no longer part of the playlist, or an Error.
     */
    get evicted(): AbortSignal<EvictionReason | Error> {

        return this.#evicted;
    }

    /** Called when this.entry is set. */
    private _update(closed: boolean, old?: IndependentSegment): void {

        if (closed) {
            (<{ isClosed: boolean }> this).isClosed = true;
            if (this.#closed) {
                this.#closed.resolve(true);
            }
        }

        if (this.onUpdate) {
            Promise.resolve().then(this.onUpdate.bind(this, this._entry, old)).catch((err) => {

                if (err.name !== 'AbortError') {
                    console.error('Unexpected onUpdate error', err);
                }
            });
        }
    }
}

export type HlsSegmentFetcherOptions = {
    /** Start from first segment, or use stream start */
    fullStream?: boolean;

    /** Initial segment ends after this date */
    startDate?: Date | string | number;

    /** Emit error if playlist is not updated for `maxStallTime` ms */
    maxStallTime?: number;

    onIndex?: (index: Readonly<M3U8Playlist>, meta: Readonly<HlsIndexMeta>) => void;

    onProblem?: (err: Error) => void;
};

/**
 * Reads an HLS media playlist, and output segments in order.
 * Live & Event playlists are refreshed as needed, and expired segments are dropped when next() is
 * not called in a timely manner.
 *
 * The processing can end in multiple ways:
 *  - regular end of playlist, and all segments have been returned - signalled with a `null` return
 *  - unrecoverable source playlist error (network / malformed content / timeout)
 *  - a user cancel()
 *
 * Any irregular processing stop, will cause the active/next call to next() to reject with an `Error`.
 * This will cause any segment entries that are pending to be returned, to be dropped.
 *
 * The returned objects are tracked for segment evictions. The evicted signal will trigger when a
 * segment is no longer available in the latest playlist update, or with an `Error` if the
 * processing is ended.
 */
export class HlsSegmentFetcher {

    readonly fullStream: boolean;
    startDate?: Date;
    stallAfterMs: number;

    readonly source: HlsPlaylistFetcher;

    #next = new SegmentPointer();
    /** Current partial segment */
    #current: HlsFetcherObject | null = null;
    #playlist?: ParsedPlaylist;
    #nextPlaylist = new Deferred<ParsedPlaylist>(true);
    #fetchUpdate = new Deferred<void>();
    #ac = new AbortController();
    #pending = false;
    #track = {
        active: new Map<string, AbortController<EvictionReason | Error>>(),
        gen: 0
    };

    constructor(source: HlsPlaylistFetcher, options: HlsSegmentFetcherOptions = {}) {

        assert(source instanceof HlsPlaylistFetcher, 'Source must be a HlsPlaylistFetcher');

        this.source = source;

        this.fullStream = !!options.fullStream;

        // Dates are inclusive

        this.startDate = options.startDate ? new Date(options.startDate) : undefined;
        this.stallAfterMs = options.maxStallTime ?? Infinity;

        if (options.onProblem) {
            this.onProblem = options.onProblem;
        }

        if (options.onIndex) {
            this.onIndex = options.onIndex;
        }

        setMaxListeners(0, this.#ac.signal);
    }

    // Public API

    /** Start fetching, returning the initial index */
    start(): Promise<Readonly<M3U8Playlist>> {

        this.#ac.signal.throwIfAborted();

        // Track aborted to immediately evict all segments

        this.#ac.signal.addEventListener('abort', () => this.#evictAll(this.#ac.signal.reason), { once: true });

        // Start feed fetcher

        const promise = this.source.index();
        this._feedFetcher(promise).catch((err) => {

            this.#nextPlaylist.reject(err);
            this.#evictAll(err);            // TODO: make this eviction optional?
        });

        return promise.then((obj) => obj.index);
    }

    /** Get the next logical segment, waiting if needed or refresh is true */
    next({ refresh }: { refresh?: boolean } = {}): Promise<HlsFetcherObject | null> {

        this.#ac.signal.throwIfAborted();
        assert(!this.#pending, 'Next segment is already being fetched');

        const promise = this._requestPlaylistUpdate();
        const ready = refresh ? promise : Promise.resolve();
        this.#pending = true;

        return ready
            .then(() => this._getNextSegment())
            // eslint-disable-next-line no-return-assign
            .finally(() => this.#pending = false);
    }

    /** A cancel() indicates loss of interest from consumer. */
    cancel(reason?: Error): void {

        if (this.#ac.signal.aborted) {
            return;
        }

        this.#ac.abort();
        this.#current?.close();
        this.#current = null;
        this.source.cancel(reason);
    }

    // Overrideable hooks

    // eslint-disable-next-line @typescript-eslint/no-empty-function
    protected onProblem(_err: Error) {}

    // eslint-disable-next-line @typescript-eslint/no-empty-function
    protected onIndex(_index: Readonly<M3U8Playlist>, _meta: Readonly<HlsIndexMeta>) {}

    // Private methods

    /** Fetch index updates in a loop, as long as there is next() interest */
    private async _feedFetcher(initial: Promise<PlaylistObject>) {

        const obj = await initial;
        this._updateIndex(obj);

        while (this.source.canUpdate()) {

            // Wait until there has been at least one new next() call, to automatically stop fetching when not actively used

            if (!this.#current) {
                await this.#fetchUpdate.promise;
            }

            const update = await this.source.update({ timeout: this.stallAfterMs });
            this._updateIndex(update);
        }
    }

    /** Trigger active fetchUpdate promise and prepare the next */
    private _requestPlaylistUpdate(): Promise<ParsedPlaylist> {

        const deferred = this.#fetchUpdate;
        this.#fetchUpdate = new Deferred<void>();
        deferred.resolve();

        return this.#nextPlaylist.promise;
    }

    private _updateIndex(input: Readonly<PlaylistObject>): void {

        const { index, playlist, meta } = input;

        if (index.master) {
            this.onIndex(index, meta);
            this.#nextPlaylist.reject(new Error('master playlist'));
            return;
        }

        assert(playlist);

        // Update evictions

        this._updateEvictions(index);

        // Immediately update active entry with latest data

        if (this.#current) {
            const currentSegment = index.getSegment(this.#current.msn, true);
            if (currentSegment) {
                this.#current.hints = currentSegment.isPartial() ? playlist.preloadHints : undefined;

                this.#current.entry = currentSegment;
                if (this.#current.isClosed) {
                    this.#current = null;
                }
            }
        }

        // Signal update

        this.onIndex(index, meta);

        // Signal new playlist is ready

        this.#playlist = playlist;
        this.#nextPlaylist.resolve(playlist);
        this.#nextPlaylist = new Deferred(true);
    }

    private _updateEvictions(index: Readonly<MediaPlaylist>) {

        const track = this.#track;

        const previous = this.#playlist?.index;
        if (previous) {
            track.gen += +(index.media_sequence < previous.media_sequence);
        }

        const incoming = new Set<string>();
        for (let i = index.startMsn(true); i <= index.lastMsn(true); ++i) {
            incoming.add(`${track.gen}-${i}`);
        }

        // Evict all expired segments

        // eslint-disable-next-line @typescript-eslint/no-non-null-asserted-optional-chain
        const waitTime = previous?.target_duration ?? +index.target_duration! * 1000;
        for (const [token, ac] of track.active) {
            if (incoming.has(token)) {
                continue;
            }

            // Wait one target duration before evicting

            wait(waitTime, { signal: this.#ac.signal })
                .then(() => ac.abort(EvictionReason.Expired), (err) => ac.abort(err));

            track.active.delete(token);
        }
    }

    #evictAll(reason: EvictionReason | Error) {

        for (const ac of this.#track.active.values()) {
            ac.abort(reason);
        }

        this.#track.active.clear();
    }

    private _segmentOffset(segment: MediaSegment) {

        if (segment.program_time && this.startDate) {
            return +segment.program_time - +this.startDate;
        }

        // Fallback to track playlist evicted segment durations + current playlist offset
        // This messes up the timeline on temporal discontinuities.

        //return this.#track.offset +
    }

    async _getNextSegment() {

        //await this.#active?.closed();

        const result = await this._getSegmentOrWait(this.#next);

        this.#current?.close();
        this.#current = null;

        if (result) {

            // Apply cross playlist discontinuity

            if (result.discont) {
                result.segment = new MediaSegment({ ...result.segment, discontinuity: true }) as IndependentSegment;
            }

            const ac = new AbortController();
            const offset = this._segmentOffset(result.segment);
            const obj = new HlsFetcherObject(result.ptr.msn, result.segment, { offset, baseUrl: this.source.baseUrl, signal: ac.signal });
            this.#track.active.set(`${this.#track.gen}-${obj.msn}`, ac);
            this.#next = result.ptr.next();

            if (result.segment.isPartial()) {
                obj.hints = this.#playlist!.preloadHints;

                // Try to fetch remainder of segment parts (in the background)

                this.#current = obj;
                this._getSegmentOrWait(new SegmentPointer(result.ptr.msn)).catch((_err) => {

                    // Ignore

                }).finally(() => {

                    obj.close();
                    if (obj === this.#current) {
                        this.#current = null;
                    }
                });
            }

            return obj;
        }

        return null;
    }

    /**
     * @argument ptr - The segment to look for. Should be within current playlist range, or 1 msn ahead of it.
     *
     * @returns specified segment, or `null` which signals that no more segments can be returned,
     * either because all segments have been exhausted from a non-updateable playlist,
     * or business logic determines it is appropriate to stop.
     */
    private async _getSegmentOrWait(ptr: SegmentPointer): Promise<{ ptr: SegmentPointer; discont: boolean; segment: IndependentSegment } | null> {

        let playlist = this.#playlist ?? await this._requestPlaylistUpdate();
        let discont = false;
        do {
            this.#ac.signal.throwIfAborted();

            if (ptr.isEmpty()) {
                ptr = this._initialSegmentPointer(playlist);
            }
            else if ((ptr.msn < playlist.index.startMsn(true)) ||
                     (ptr.msn > (playlist.index.lastMsn(this.source.lowLatency) + 1 + +(ptr.part === 0)))) {

                // Playlist jump

                if (playlist.index.type /* VOD or event */) {
                    throw new Error('Fatal playlist inconsistency');
                }

                this.onProblem(new Error('Playlist jump'));

                ptr = new SegmentPointer(playlist.index.startMsn(true), this.source.lowLatency ? 0 : undefined);
                discont = true;
            }

            const segment = playlist.index.getSegment(ptr.msn, true);
            if (!segment ||
                (ptr.part === undefined && segment.isPartial()) ||
                (ptr.part && ptr.part >= (segment?.parts?.length || 0))) {

                if (!playlist.index.isLive() || !this.source.canUpdate()) {
                    break;        // Done - nothing more to do
                }

                if (!segment && ptr.part !== undefined && playlist.preloadHints.part && ptr.msn === playlist.nextHead().msn) {
                    return { ptr, discont, segment: new HintedSegment({ parts: [] }) as IndependentSegment };  // Return empty segment for upcoming hint
                }

                continue;         // Try again
            }

            // Set startDate if not yet specified

            if (!this.startDate) {
                this.startDate = segment.program_time ?? undefined;
            }

            return { ptr, discont, segment };
        } while (playlist = await this._requestPlaylistUpdate());

        return null;
    }

    private _initialSegmentPointer(playlist: ParsedPlaylist): SegmentPointer {

        if (!this.fullStream && this.startDate) {
            const msn = playlist.index.msnForDate(this.startDate, true);
            if (msn >= 0) {
                return new SegmentPointer(msn, this.source.lowLatency ? 0 : undefined);
            }

            // no date information in index
        }

        if (this.source.lowLatency && playlist.serverControl.partHoldBack) {
            let partHoldBack = playlist.serverControl.partHoldBack;
            let msn = playlist.index.lastMsn(true);
            let segment;
            while (segment = playlist.index.getSegment(msn)) {
                // TODO: use INDEPENDENT=YES information for better start point

                if (segment.uri) {
                    partHoldBack -= segment.duration || 0;
                }
                else if (segment.parts) {
                    partHoldBack -= segment.parts.reduce((duration, part) => duration + part.get('duration', AttrList.Types.Float)!, 0);
                }

                if (partHoldBack <= 0) {
                    break;
                }

                msn--;
            }

            return new SegmentPointer(msn, 0);
        }

        // TODO: use start offset, when present

        return new SegmentPointer(playlist.index.startMsn(this.fullStream), this.source.lowLatency ? 0 : undefined);
    }
}
