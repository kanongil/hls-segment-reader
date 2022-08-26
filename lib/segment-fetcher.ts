import type { ParsedPlaylist, PlaylistObject } from 'hls-playlist-reader/lib/fetcher';

import { MediaPlaylist, MasterPlaylist, MediaSegment, IndependentSegment, AttrList } from 'm3u8parse';

import { HlsIndexMeta, HlsPlaylistFetcher, HlsPlaylistFetcherOptions } from 'hls-playlist-reader';
import { assert, Deferred } from 'hls-playlist-reader/lib/helpers';


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

    protected preprocessIndex<T extends MediaPlaylist | MasterPlaylist>(index: T): T | undefined {

        if (!index.master) {
            MediaPlaylist.cast(index).ended = false;
        }

        return super.preprocessIndex(index);
    }
}

export class HlsFetcherObject {

    readonly msn: number;
    readonly isClosed: boolean;

    onUpdate?: ((entry: IndependentSegment, old?: IndependentSegment) => void) = undefined;

    private _entry: IndependentSegment;
    #closed?: Deferred<true>;

    constructor(msn: number, segment: IndependentSegment) {

        this.msn = msn;
        this._entry = new MediaSegment(segment) as IndependentSegment;
        this.isClosed = !segment.isPartial();
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

    closed(): PromiseLike<true> | true {

        if (this.isClosed) {
            return true;
        }

        if (!this.#closed) {
            this.#closed = new Deferred<true>();
        }

        return this.#closed.promise;
    }

    abandon(): void {

        if (!this.isClosed) {
            return this._update(true);
        }
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

    onIndex?: (index: Readonly<MasterPlaylist | MediaPlaylist>, meta: Readonly<HlsIndexMeta>) => void;
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
    #active: HlsFetcherObject | null = null;
    #playlist?: ParsedPlaylist;
    #nextPlaylist = new Deferred<ParsedPlaylist>(true);
    #fetchUpdate = new Deferred<void>();
    #cancelled = false;
    #pending = false;

    constructor(src: string, options: HlsSegmentFetcherOptions = {}) {

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
    start(): Promise<Readonly<MasterPlaylist | MediaPlaylist>> {

        const promise = this.source.index();
        this._feedFetcher(promise).catch((err) => {

            this.#nextPlaylist.reject(err);
        });

        return promise.then((obj) => obj.index);
    }

    /** Get the next logical segment, waiting if needed or refresh is true */
    next({ refresh }: { refresh?: boolean } = {}): Promise<HlsFetcherObject | null> {

        assert(!this.#cancelled, 'Fetcher is cancelled');
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

        if (this.#cancelled) {
            return;
        }

        this.#cancelled = true;
        this.#active?.abandon();
        this.#active = null;
        this.source.cancel(reason);
    }

    // Overrideable hooks

    protected onProblem(err: Error) {

        err;      // Ignore by default
    }

    protected onIndex(index: Readonly<MasterPlaylist | MediaPlaylist>, meta: Readonly<HlsIndexMeta>) {

        index; meta;    // Ignore by default
    }

    // Private methods

    /** Fetch index updates in a loop, as long as there is next() interest */
    private async _feedFetcher(initial: Promise<PlaylistObject>) {

        const obj = await initial;
        this.writeNext(obj);

        while (this.source.canUpdate()) {

            // Wait until there has been at least one new next() call, to automatically stop fetching when not actively used

            if (!this.#active) {
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

        // Immediately update active entry with latest data

        if (this.#active) {
            const currentSegment = index.getSegment(this.#active.msn, true);
            if (currentSegment && (currentSegment.isPartial() || currentSegment.parts)) {
                this.#active.entry = currentSegment;
                if (this.#active.isClosed) {
                    this.#active = null;
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

    async _getNextSegment() {

        //await this.#active?.closed();

        const result = await this._getSegmentOrWait(this.#next);

        this.#active?.abandon();
        this.#active = null;

        if (result) {
            // Apply cross playlist discontinuity

            if (result.discont) {
                result.segment.discontinuity = true;
            }

            const obj = new HlsFetcherObject(result.ptr.msn, result.segment);
            this.#next = result.ptr.next();

            if (result.segment.isPartial()) {

                // Try to fetch remainder of segment parts (in the background)

                this.#active = obj;
                this._getSegmentOrWait(new SegmentPointer(result.ptr.msn)).catch((/*err*/) => {

                    // Ignore

                }).finally(() => {

                    obj.abandon();
                    if (obj === this.#active) {
                        this.#active = null;
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
            assert(!this.#cancelled);

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
                    partHoldBack -= segment.parts.reduce((duration, part) => duration + part.get('duration', AttrList.Types.Float), 0);
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
