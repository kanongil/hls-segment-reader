import { assert as hoekAssert } from '@hapi/hoek';
import { MediaPlaylist, MasterPlaylist, MediaSegment, IndependentSegment, AttrList } from 'm3u8parse';

import { Deferred } from 'hls-playlist-reader/lib/helpers';
import { DuplexEvents, TypedDuplex, TypedEmitter } from 'hls-playlist-reader/lib/raw/typed-readable';
import { HlsIndexMeta, HlsPlaylistFetcher, HlsPlaylistFetcherOptions } from 'hls-playlist-reader';
import type { ParsedPlaylist, PartData, PlaylistObject } from 'hls-playlist-reader/lib/fetcher';


// eslint-disable-next-line func-style
function assert(condition: any, ...args: any[]): asserts condition {

    hoekAssert(condition, ...args);
}


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

export class HlsReaderObject {

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
            process.nextTick(this.onUpdate.bind(this, this._entry, old));
        }
    }
}

export type HlsSegmentReaderOptions = {
    /** Start from first segment, or use stream start */
    fullStream?: boolean;

    /** Initial segment ends after this date */
    startDate?: Date | string | number;

    /** End when segment start after this date */
    stopDate?: Date | string | number;

    /** Emit error if playlist is not updated for `maxStallTime` ms */
    maxStallTime?: number;
} & HlsPlaylistFetcherOptions;


const HlsSegmentReaderEvents = <IHlsSegmentReaderEvents & DuplexEvents<HlsReaderObject>>(null as any);
interface IHlsSegmentReaderEvents {
    index(index: Readonly<MasterPlaylist | MediaPlaylist>, meta: Readonly<HlsIndexMeta>): void;
    hints(part?: PartData, map?: PartData): void;
    problem(err: Error): void;
}

/**
 * Reads an HLS media playlist, and output segments in order.
 * Live & Event playlists are refreshed as needed, and expired segments are dropped when backpressure is applied.
 */
export class HlsSegmentReader extends TypedEmitter(HlsSegmentReaderEvents, TypedDuplex<Readonly<PlaylistObject>, HlsReaderObject>()) {

    readonly fullStream: boolean;
    startDate?: Date;
    stopDate?: Date;
    stallAfterMs: number;

    readonly fetcher: HlsPlaylistFetcher;

    #next = new SegmentPointer();
    #current: HlsReaderObject | null = null;
    #playlist?: ParsedPlaylist;
    #nextPlaylist = new Deferred<ParsedPlaylist>(true);
    #needRead = new Deferred<void>();
    #readActive = false;
    #index?: Readonly<MediaPlaylist | MasterPlaylist>;

    constructor(src: string, options: HlsSegmentReaderOptions = {}) {

        super({ objectMode: true, highWaterMark: 0, autoDestroy: true, emitClose: true, allowHalfOpen: false });

        this.fullStream = !!options.fullStream;

        // Dates are inclusive

        this.startDate = options.startDate ? new Date(options.startDate) : undefined;
        this.stopDate = options.stopDate ? new Date(options.stopDate) : undefined;
        this.stallAfterMs = options.maxStallTime ?? Infinity;

        const onProblem = (err: Error) => !this.destroyed && this.emit<'problem'>('problem', err);

        this.fetcher = new (this.stopDate ? UnendingPlaylistFetcher : HlsPlaylistFetcher)(src, { ...options, onProblem });

        this._feedFetcher(this.fetcher.index()).catch((err) => {

            // Must defer to NT since this.readableEnded might already have been scheduled

            process.nextTick(() => this.destroy(this.readableEnded ? undefined : err)); // FIXME: really needed??
        });
    }

    private async _feedFetcher(initial: Promise<PlaylistObject>) {

        const write = (data: PlaylistObject): Promise<void> | void => {

            this.#index = data.index;

            if (!this.write(data)) {
                const deferred = new Deferred<void>();
                this.once('drain', deferred.resolve);
                return deferred.promise;
            }
        };

        const obj = await initial;
        await write(obj);

        while (this.fetcher.canUpdate()) {
            const update = await this.fetcher.update({ timeout: this.stallAfterMs });
            await write(update);
        }

        this.end();
    }

    get index(): Readonly<MediaPlaylist | MasterPlaylist> | undefined {

        return this.#index;
    }

    async _write(input: Readonly<PlaylistObject>, _: unknown, done: (err?: Error) => void): Promise<void> {

        try {
            const { index, playlist, meta } = input;

            if (index.master) {
                process.nextTick(this.emit.bind(this, 'index', index, meta));
                process.nextTick(this.#nextPlaylist.reject, new Error('master playlist'));
                return;
            }

            assert(input.playlist);

            // Update current entry with latest data

            if (this.#current && !this.#current.isClosed) {
                const currentSegment = index.getSegment(this.#current.msn, true);
                if (currentSegment && (currentSegment.isPartial() || currentSegment.parts)) {
                    this.#current.entry = currentSegment;
                }
            }

            // Emit updates

            process.nextTick(this.emit.bind(this, 'index', index, meta));

            // Signal new playlist is ready

            this.#playlist = playlist;
            process.nextTick(this.#nextPlaylist.resolve, playlist);
            this.#nextPlaylist = new Deferred(true);

            // Wait until output side needs more segments

            if (index.isLive()) {
                await this.#needRead.promise;
            }
        }
        catch (err: any) {
            //this.#nextPlaylist.reject(err);
            return done(err);
        }

        return done();
    }

    /**
     * Called to push the next segment.
     */
    /*protected*/ async _read(): Promise<void> {

        if (this.#readActive) {
            return;
        }

        this.#readActive = true;
        try {
            const deferred = this.#needRead;
            this.#needRead = new Deferred();
            deferred.resolve();

            let more = true;
            while (more) {
                try {
                    const result = await this._getNextSegment(this.#next);

                    this.#current?.abandon();

                    if (!result) {
                        this.#current = null;
                        this.push(null);
                        this.fetcher.cancel();
                        return;
                    }

                    // Apply cross playlist discontinuity

                    if (result.discont) {
                        result.segment.discontinuity = true;
                    }

                    this.#current = new HlsReaderObject(result.ptr.msn, result.segment);
                    this.#next = result.ptr.next();

                    this.#readActive = more = this.push(this.#current);

                    if (result.segment.isPartial()) {
                        more || (more = !await this._getNextSegment(new SegmentPointer(result.ptr.msn))); // fetch until we have the full segment
                    }
                }
                catch (err: any) {
                    if (this.index) {
                        if (this.index.master) {
                            this.push(null);                    // Just ignore any error
                            this.fetcher.cancel();
                            return;
                        }

                        if (this.fetcher.isRecoverableUpdateError(err)) {
                            continue;
                        }
                    }

                    throw err;
                }
            }
        }
        catch (err: any) {
            this.destroy(err);
        }
        finally {
            this.#readActive = false;
        }
    }

    _destroy(err: Error | null, cb: unknown): void {

        this.#current?.abandon();
        this.fetcher.cancel();

        super._destroy(err, cb as any);
    }

    // Private methods

    private async _waitForUpdate(from?: Readonly<MediaPlaylist>): Promise<ParsedPlaylist> {

        if (this.index?.master) {
            throw new Error('Master playlist cannot be updated');
        }

        let playlist = this.#playlist;
        while (!this.destroyed) {
            if (playlist) {
                const updated = !from || !playlist.index.isLive() || !playlist.isSameHead(from);
                if (updated) {
                    return playlist;
                }
            }

            // Signal stall

            const deferred = this.#needRead;
            this.#needRead = new Deferred();
            deferred.resolve();

            // Wait for new playlist

            playlist = await this.#nextPlaylist.promise;
        }

        throw new Error('Stream was destroyed');
    }

    private async _getNextSegment(ptr: SegmentPointer): Promise<{ ptr: SegmentPointer; discont: boolean; segment: IndependentSegment } | null> {

        let playlist: ParsedPlaylist | undefined;
        let discont = false;
        while (playlist = await this._waitForUpdate(playlist?.index)) {
            if (ptr.isEmpty()) {
                ptr = this._initialSegmentPointer(playlist);
            }
            else if ((ptr.msn < playlist.index.startMsn(true)) ||
                     (ptr.msn > (playlist.index.lastMsn(this.fetcher.lowLatency) + 1))) {

                // Playlist jump

                if (playlist.index.type /* VOD or event */) {
                    throw new Error('Fatal playlist inconsistency');
                }

                this.emit<'problem'>('problem', new Error('Playlist jump'));

                ptr = new SegmentPointer(playlist.index.startMsn(true), this.fetcher.lowLatency ? 0 : undefined);
                discont = true;
            }

            const segment = playlist.index.getSegment(ptr.msn, true);
            if (!segment ||
                (ptr.part === undefined && segment.isPartial()) ||
                (ptr.part && ptr.part >= (segment?.parts?.length || 0))) {

                if (!playlist.index.isLive()) {
                    return null;        // Done - nothing more to do
                }

                continue;               // Try again
            }

            // Check if we need to stop

            if (this.stopDate && (segment.program_time || 0) > this.stopDate) {
                return null;
            }

            return { ptr, discont, segment };
        }

        return null;
    }

    private _initialSegmentPointer(playlist: ParsedPlaylist): SegmentPointer {

        if (!this.fullStream && this.startDate) {
            const msn = playlist.index.msnForDate(this.startDate, true);
            if (msn >= 0) {
                return new SegmentPointer(msn, this.fetcher.lowLatency ? 0 : undefined);
            }

            // no date information in index
        }

        if (this.fetcher.lowLatency && playlist.serverControl.partHoldBack) {
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

        return new SegmentPointer(playlist.index.startMsn(this.fullStream), this.fetcher.lowLatency ? 0 : undefined);
    }
}
