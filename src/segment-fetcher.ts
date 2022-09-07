/// <reference lib="es2021.weakref" />

import type { ParsedPlaylist } from 'hls-playlist-reader/playlist';

import { M3U8Playlist, MediaPlaylist, MediaSegment, IndependentSegment, AttrList } from 'm3u8parse';

import { HlsIndexMeta, HlsPlaylistFetcher, HlsPlaylistFetcherOptions, PlaylistObject } from 'hls-playlist-reader/fetcher';
import { AbortController, assert, Deferred, wait } from 'hls-playlist-reader/helpers';

const WeakRef = globalThis.WeakRef ?? await import('./weakref.ponyfill.js');


class SegmentPointer {

    readonly msn: number;
    readonly part?: number;

    constructor(msn = -1, part?: number) {

        this.msn = +msn;
        this.part = part;
    }

    next(isPartial = false): SegmentPointer {

        if (isPartial) {
            return new SegmentPointer(this.msn, undefined);
        }

        return new SegmentPointer(this.msn + 1, this.part === undefined ? undefined : 0);
    }

    isEmpty(): boolean {

        return this.msn < 0;
    }
}

class UnendingPlaylistFetcher extends HlsPlaylistFetcher {

    protected preprocessIndex<T extends M3U8Playlist>(index: T): T | undefined {

        if (!index.master) {
            MediaPlaylist.cast(index).ended = false;
        }

        return super.preprocessIndex(index);
    }
}

export class HlsFetcherObject {

    readonly msn: number;
    readonly isClosed: boolean;
    readonly baseUrl: string;

    onUpdate?: ((entry: IndependentSegment, old?: IndependentSegment) => void) = undefined;

    private _entry: IndependentSegment;
    #closed?: Deferred<true>;
    #evicted = new AbortController();

    constructor(msn: number, segment: IndependentSegment, baseUrl: string) {

        this.msn = msn;
        this._entry = new MediaSegment(segment) as IndependentSegment;
        this.isClosed = !segment.isPartial();
        this.baseUrl = baseUrl;
    }

    get entry(): IndependentSegment {

        return this._entry;
    }

    set entry(entry: IndependentSegment) {

        assert(!this.isClosed);

        const old = this._entry;
        this._entry = new MediaSegment(entry) as IndependentSegment;

        this._entry.discontinuity = !!(+entry.discontinuity | +old.discontinuity);

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

    /**
     * Triggers once the segment is evicted from the parent index.
     */
    get evicted(): AbortSignal {

        return this.#evicted.signal;
    }

    // For internal usage

    _close(): void {

        if (!this.isClosed) {
            return this._update(true);
        }
    }

    _evict(): void {

        this.#evicted.abort();      // TODO: add an eviction delay??? Eg. segment duration??
    }

    private _update(closed: boolean, old?: IndependentSegment): void {

        if (closed) {
            (<{ isClosed: boolean }> this).isClosed = true;
            if (this.#closed) {
                this.#closed.resolve(true);
            }
        }

        if (this.onUpdate) {
            Promise.resolve().then(this.onUpdate.bind(this, this._entry, old));
        }
    }
}

export type HlsSegmentFetcherOptions = {
    /** Start from first segment, or use stream start */
    fullStream?: boolean;

    /** Initial segment ends after this date */
    startDate?: Date | string | number;

    /** End when segment start after this date */
    stopDate?: Date | string | number;

    /** Emit error if playlist is not updated for `maxStallTime` ms */
    maxStallTime?: number;

    onIndex?: (index: Readonly<M3U8Playlist>, meta: Readonly<HlsIndexMeta>) => void;
} & HlsPlaylistFetcherOptions;


/**
 * Reads an HLS media playlist, and output segments in order.
 * Live & Event playlists are refreshed as needed, and expired segments are dropped when backpressure is applied.
 */
export class HlsSegmentFetcher {

    readonly fullStream: boolean;
    startDate?: Date;
    stopDate?: Date;
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
        active: new Map<string, WeakRef<HlsFetcherObject>>(),
        gen: 0,
        startMsn: -1
    };

    constructor(src: string | URL, options: HlsSegmentFetcherOptions = {}) {

        this.fullStream = !!options.fullStream;

        // Dates are inclusive

        this.startDate = options.startDate ? new Date(options.startDate) : undefined;
        this.stopDate = options.stopDate ? new Date(options.stopDate) : undefined;
        this.stallAfterMs = options.maxStallTime ?? Infinity;

        if (options.onProblem) {
            this.onProblem = options.onProblem;
        }

        if (options.onIndex) {
            this.onIndex = options.onIndex;
        }

        this.source = new (this.stopDate ? UnendingPlaylistFetcher : HlsPlaylistFetcher)(src, { ...options, onProblem: this.onProblem.bind(this) });
    }

    // Public API

    /** Start fetching, returning the initial index */
    start(): Promise<Readonly<M3U8Playlist>> {

        const promise = this.source.index();
        this._feedFetcher(promise).catch((err) => {

            this.#nextPlaylist.reject(err);
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

    cancel(reason?: Error): void {

        if (this.#ac.signal.aborted) {
            return;
        }

        this.#ac.abort();
        this.#current?._close();
        this.#current = null;
        this.source.cancel(reason);
        this._updateEvictions();
    }

    // Overrideable hooks

    protected onProblem(_err: Error) {}

    protected onIndex(_index: Readonly<M3U8Playlist>, _meta: Readonly<HlsIndexMeta>) {}

    // Private methods

    /** Fetch index updates in a loop, as long as there is next() interest */
    private async _feedFetcher(initial: Promise<PlaylistObject>) {

        const obj = await initial;
        this.writeNext(obj);

        while (this.source.canUpdate()) {

            // Wait until there has been at least one new next() call, to automatically stop fetching when not actively used

            if (!this.#current) {
                await this.#fetchUpdate.promise;
            }

            const update = await this.source.update({ timeout: this.stallAfterMs });
            this.writeNext(update);
        }
    }

    /** Trigger active fetchUpdate promise and prepare the next */
    private _requestPlaylistUpdate(): Promise<ParsedPlaylist> {

        const deferred = this.#fetchUpdate;
        this.#fetchUpdate = new Deferred<void>();
        deferred.resolve();

        return this.#nextPlaylist.promise;
    }

    private writeNext(input: Readonly<PlaylistObject>): void {

        const { index, playlist, meta } = input;

        if (index.master) {
            this.onIndex(index, meta);
            this.#nextPlaylist.reject(new Error('master playlist'));
            return;
        }

        assert(input.playlist);

        // Update evictions

        this._updateEvictions(index)

        // Immediately update active entry with latest data

        if (this.#current) {
            const currentSegment = index.getSegment(this.#current.msn, true);
            if (currentSegment && (currentSegment.isPartial() || currentSegment.parts)) {
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

    private _updateEvictions(index?: Readonly<MediaPlaylist>) {

        const track = this.#track;
        const incoming = new Set<string>();

        if (index) {
            track.gen += +(index.media_sequence < track.startMsn);
            track.startMsn = index.media_sequence;

            for (let i = index.startMsn(true); i <= index.lastMsn(true); ++i) {
                incoming.add(`${track.gen}-${i}`);
            }
        }

        // Evict all expired segments

        const waitTime = +index?.target_duration! * 1000;
        for (const [token, ref] of track.active) {
            if (!incoming.has(token)) {
                const segment = ref.deref();
                if (segment) {
                    if (index) {

                        // Wait one target duration before evicting

                        wait(segment.entry.duration! * 1000, { signal: this.#ac.signal })
                            .catch(() => undefined)
                            .then(() => ref.deref()?._evict());
                    }
                    else {
                        segment._evict();
                    }
                }

                incoming.delete(token);
            }
        }
    }

    async _getNextSegment() {

        //await this.#active?.closed();

        const result = await this._getSegmentOrWait(this.#next);

        this.#current?._close();
        this.#current = null;

        if (result) {

            // Apply cross playlist discontinuity

            if (result.discont) {
                result.segment.discontinuity = true;
            }

            const obj = new HlsFetcherObject(result.ptr.msn, result.segment, this.source.baseUrl);
            this.#next = result.ptr.next();

            if (result.segment.isPartial()) {

                // Try to fetch remainder of segment parts (in the background)

                this.#current = obj;
                this._getSegmentOrWait(new SegmentPointer(result.ptr.msn)).catch((/*err*/) => {

                    // Ignore

                }).finally(() => {

                    obj._close();
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
                     (ptr.msn > (playlist.index.lastMsn(this.source.lowLatency) + 1))) {

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

                if (!playlist.index.isLive()) { // TODO: also check fetcher???
                    break;        // Done - nothing more to do
                }

                continue;         // Try again
            }

            // Check if we need to stop due to options

            if (this.stopDate && (segment.program_time || 0) > this.stopDate) {
                break;
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
                else {
                    assert(segment.parts);
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
