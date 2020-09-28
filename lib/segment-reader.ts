import type { Stream } from 'stream';
import type { FetchResult, PartData } from './helpers';

import { hrtime } from 'process';
import { URL } from 'url';

import { Boom } from '@hapi/boom';
import { assert as hoekAssert, ignore, wait } from '@hapi/hoek';
import M3U8Parse, { MediaPlaylist, MasterPlaylist, MediaSegment, IndependentSegment, AttrList, ParserError } from 'm3u8parse';
import { Readable } from 'readable-stream';

import { Deferred, ParsedPlaylist, FsWatcher, performFetch } from './helpers';
import { TypedReadable, ReadableEvents, TypedEmitter } from './raw/typed-readable';


// eslint-disable-next-line func-style
function assert(condition: any, ...args: any[]): asserts condition {

    hoekAssert(condition, ...args);
}


type Hint = {
    uri: string;
    byterange?: {
        offset: number;
        length?: number;
    };
};


const internals = {
    indexMimeTypes: new Set([
        'application/vnd.apple.mpegurl',
        'application/x-mpegurl',
        'audio/mpegurl'
    ]),

    isSameHint(h1?: Hint, h2?: Hint): boolean {

        if (h1 === undefined || h2 === undefined) {
            return h1 === h2;
        }

        if (h1.uri !== h2.uri) {
            return false;
        }

        if (h1.byterange && h2.byterange) {
            if (h1.byterange.offset !== h2.byterange.offset ||
                h1.byterange.length !== h2.byterange.length) {
                return false;
            }
        }
        else if (h1.byterange !== h2.byterange) {
            return false;
        }

        return true;
    }
};


class SegmentPointer {

    msn: number;
    part?: number;

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

    /** True to handle LL-HLS streams */
    lowLatency?: boolean;

    startDate?: Date | string | number;
    stopDate?: Date | string | number;

    maxStallTime?: number;

    extensions?: { [K: string]: boolean };
};


export type HlsIndexMeta = {
    url: string;
    modified?: Date;
};

const HlsReaderObjectType = <HlsReaderObject>(null as any);
const HlsSegmentReaderEvents = <IHlsSegmentReaderEvents & ReadableEvents<HlsReaderObject>>(null as any);
interface IHlsSegmentReaderEvents {
    index(index: MediaPlaylist | MasterPlaylist, meta: HlsIndexMeta): void;
    hints(part: PartData, map: PartData): void;
    problem(err: Error): void;
}

/**
 * Reads an HLS media playlist, and output segments in order.
 * Live & Event playlists are refreshed as needed, and expired segments are dropped when backpressure is applied.
 */
export class HlsSegmentReader extends TypedReadable(HlsReaderObjectType, TypedEmitter(HlsSegmentReaderEvents, Readable)) {

    static readonly recoverableCodes = new Set<number>([
        404, // Not Found
        408, // Request Timeout
        425, // Too Early
        429 // Too Many Requests
    ]);

    readonly url: URL;
    readonly fullStream: boolean;
    readonly lowLatency: boolean;
    readonly extensions: HlsSegmentReaderOptions['extensions'];
    startDate?: Date;
    stopDate?: Date;
    stallAfterMs: number;

    readonly baseUrl: string;
    readonly modified?: Date;

    #next = new SegmentPointer();
    #discont = false;
    #rejected = 0;
    #indexStalledAt: bigint | null = null;
    #index?: MediaPlaylist | MasterPlaylist;
    #playlist?: ParsedPlaylist;
    #current: HlsReaderObject | null = null;
    #currentHints: ParsedPlaylist['preloadHints'] = {};
    #nextUpdate?: Promise<void>;
    #fetch?: ReturnType<typeof performFetch>;
    #watcher?: FsWatcher;

    constructor(src: string, options: HlsSegmentReaderOptions = {}) {

        super({ objectMode: true, highWaterMark: 0 });

        this.url = new URL(src);
        this.baseUrl = src;

        this.fullStream = !!options.fullStream;
        this.lowLatency = !!options.lowLatency;

        // Dates are inclusive

        this.startDate = options.startDate ? new Date(options.startDate) : undefined;
        this.stopDate = options.stopDate ? new Date(options.stopDate) : undefined;

        this.stallAfterMs = options.maxStallTime ?? Infinity;

        this.extensions = options.extensions ?? {};

        this._keepIndexUpdated();
    }

    get index(): MediaPlaylist | MasterPlaylist | undefined {

        return this.#index;
    }

    get hints(): ParsedPlaylist['preloadHints'] {

        return this.#currentHints;
    }

    get indexMimeTypes(): Set<string> {

        return internals.indexMimeTypes;
    }

    validateIndexMeta(meta: FetchResult['meta']): void | never {

        // Check for valid mime type

        if (!this.indexMimeTypes.has(meta.mime.toLowerCase()) &&
            meta.url.indexOf('.m3u8', meta.url.length - 5) === -1 &&
            meta.url.indexOf('.m3u', meta.url.length - 4) === -1) {

            throw new Error('Invalid MIME type: ' + meta.mime); // TODO: make recoverable
        }
    }

    /**
     * Returns whether another attempt might fix the update error.
     *
     * The test is quite lenient since this will only be called for resources that have previously
     * been accessed without an error.
     */
    isRecoverableUpdateError(err: Error): boolean {

        const { recoverableCodes } = HlsSegmentReader;

        if (err instanceof Boom) {
            if (err.isServer || recoverableCodes.has(err.output.statusCode)) {
                return true;
            }
        }

        if (err instanceof ParserError) {
            return true;
        }

        if ((err as any).syscall) {      // Any syscall error
            return true;
        }

        return false;
    }

    /**
     * Resolves once there is an index with a different head, than the passed one.
     *
     * @param index - Current index to compare against
     */
    async waitForUpdate(index?: MediaPlaylist): Promise<undefined> {

        while (!this.destroyed) {
            if (this.index && !this.#playlist) {
                throw new Error('Master playlist cannot be updated');
            }

            if (this.#playlist) {
                const updated = !index || this.#playlist.index.ended || !this.#playlist.isSameHead(index, this.lowLatency);
                if (updated) {
                    return;
                }
            }

            // We need to wait

            await this.#nextUpdate;
        }

        throw new Error('Stream was destroyed');
    }

    /**
     * Called to push the next segment.
     */
    /*protected*/ async _read(): Promise<void> {

        try {
            let ready = true;
            while (ready) {
                try {
                    const last = this.#current;
                    this.#current = await this._getNextSegment();
                    last?.abandon();
                    ready = this.push(this.#current);
                }
                catch (err) {
                    if (this.index) {
                        if (this.index.master) {
                            this.push(null);                    // Just ignore any error
                            return;
                        }

                        if (this.isRecoverableUpdateError(err)) {
                            continue;
                        }
                    }

                    throw err;
                }
            }
        }
        catch (err) {
            if (!this.destroyed) {
                this.destroy(err);
            }
        }
    }

    /*protected*/ _destroy(err: Error | null, cb: unknown): void {

        super._destroy(err, cb as any);

        if (this.#fetch) {
            this.#fetch.abort();
            this.#fetch = undefined;
        }

        if (this.#watcher) {
            this.#watcher.close();
            this.#watcher = undefined;
        }
    }

    // Private methods

    private async _getNextSegment(): Promise<HlsReaderObject | null> {

        let playlist;
        while (playlist = await this._waitForUpdate(playlist)) {
            if (this.#next.isEmpty()) {
                this.#next = this._initialSegmentPointer(playlist);
            }
            else if ((this.#next.msn < playlist.index.startMsn(true)) ||
                     (this.#next.msn > (playlist.index.lastMsn(this.lowLatency) + 1))) {

                // Playlist jump

                if (playlist.index.type /* VOD or event */) {
                    throw new Error('Fatal playlist inconsistency');
                }

                this.emit('problem', new Error('Playlist jump'));

                this.#next = new SegmentPointer(playlist.index.startMsn(true), this.lowLatency ? 0 : undefined);
                this.#discont = true;
            }

            const next = this.#next;
            const segment = playlist.index.getSegment(next.msn, true);
            if (!segment ||
                (next.part === undefined && segment.isPartial()) ||
                (next.part && next.part >= (segment?.parts?.length || 0))) {

                if (playlist.index.ended) {
                    return null;        // Done - nothing more to do
                }

                continue;               // Try again
            }

            // Check if we need to stop

            if (this.stopDate && (segment.program_time || 0) > this.stopDate) {
                return null;
            }

            // Apply cross playlist discontinuity

            if (this.#discont) {
                segment.discontinuity = true;
                this.#discont = false;
            }

            // Advance next

            this.#next = next.next();

            return new HlsReaderObject(next.msn, segment);
        }

        return null;
    }

    private async _waitForUpdate(playlist?: ParsedPlaylist): Promise<ParsedPlaylist> {

        await this.waitForUpdate(playlist?.index);
        return this.#playlist!;
    }

    /**
     * Throws if method has not been called with updated === true for options.stallAfterMs
     */
    private _stallCheck(updated = false): void | never {

        if (updated) {
            this.#indexStalledAt = null;
        }
        else {
            if (this.#indexStalledAt === null) {
                this.#indexStalledAt = hrtime.bigint();      // Record when stall began
            }
            else {
                if (Number((hrtime.bigint() - this.#indexStalledAt) / BigInt(1000000)) > this.stallAfterMs) {
                    throw new Error('Index update stalled');
                }
            }
        }
    }

    protected _getUpdateInterval({ index, partTarget }: ParsedPlaylist, updated = false): number {

        let updateInterval = index.target_duration!;
        if (this.lowLatency && partTarget! > 0 && !index.i_frames_only) {
            updateInterval = partTarget!;
        }

        if (!updated || !index.segments.length) {
            updateInterval /= 2;
        }

        return updateInterval;
    }

    private _initialSegmentPointer(playlist: ParsedPlaylist): SegmentPointer {

        if (!this.fullStream && this.startDate) {
            const msn = playlist.index.msnForDate(this.startDate, true);
            if (msn >= 0) {
                return new SegmentPointer(msn, this.lowLatency ? 0 : undefined);
            }

            // no date information in index
        }

        if (this.lowLatency && playlist.serverControl.partHoldBack) {
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

        return new SegmentPointer(playlist.index.startMsn(this.fullStream), this.lowLatency ? 0 : undefined);
    }

    protected _preprocessIndex<T extends MediaPlaylist | MasterPlaylist>(index: T): T | undefined {

        // Reject "old" index updates (eg. from CDN cached response & hitting multiple endpoints)

        if (this.index && !this.index.master && this.#rejected < 3) {
            if (MediaPlaylist.cast(index).lastMsn(true) < this.index.lastMsn(true)) {
                this.#rejected++;
                //this.emit('problem', new Error('Rejected update from the past')); // TODO: make recoverable
                return this.index as T;
            }

            // TODO: reject other strange updates??
        }

        this.#rejected = 0;

        /*if (!this.lowLatency) {

            // Ignore partial-only segment

            if (index.segments.length && !index.segments[index.segments.length - 1].uri) {
                index.segments.pop();
            }

            // TODO: strip all low-latency
        }*/

        return index;
    }

    private _updateHints(playlist?: ParsedPlaylist) {

        if (!this.lowLatency || !playlist) {
            return;
        }

        const hints = playlist.preloadHints;
        if (!internals.isSameHint(hints.part, this.#currentHints.part)) {
            this.#currentHints = hints;
            if (playlist.serverControl.canBlockReload) {
                process.nextTick(this.emit.bind(this, 'hints' as any, hints.part, hints.map));
            }
        }
    }

    private _keepIndexUpdated() {

        assert(!this.#nextUpdate);

        const createFetch = (url: URL) => {

            return (this.#fetch = performFetch(url, { timeout: 30 * 1000 }));
        };

        const delayedUpdate = async (fromPlaylist: ParsedPlaylist, wasUpdated: boolean, wasError = false) => {

            let delayMs = this._getUpdateInterval(fromPlaylist, wasUpdated && !wasError) * 1000;

            const url = new URL(this.url as any);
            if (url.protocol === 'data:') {
                throw new Error('data: uri cannot be updated');
            }

            // Apply SERVER-CONTROL, if available

            if (!wasError && fromPlaylist.serverControl.canBlockReload) {
                const head = fromPlaylist.nextHead(this.lowLatency);

                // TODO: detect when playlist is behind server, and guess future part instead / CDN tunein

                // Params must appear in UTF-8 order

                url.searchParams.set('_HLS_msn', `${head.msn}`);
                if (head.part !== undefined) {
                    url.searchParams.set('_HLS_part', `${head.part}`);
                }

                delayMs = 0;
            }

            if (delayMs && this.#watcher) {
                try {
                    await Promise.race([wait(delayMs), this.#watcher.next()]);
                }
                catch (err) {
                    this.emit('problem', err);
                    this.#watcher = undefined;
                }
            }
            else {
                await wait(delayMs);
            }

            assert(!this.destroyed, 'destroyed');

            await performUpdate(createFetch(url), fromPlaylist.index);
        };

        /**
         * Runs in a loop until there are no more updates, or stream is destroyed
         */
        const performUpdate = async (fetchPromise: ReturnType<typeof performFetch>, fromIndex?: MediaPlaylist): Promise<void> => {

            let updated = !fromIndex;
            let errored = false;
            try {
                // eslint-disable-next-line no-var
                var { meta, stream } = await fetchPromise;

                assert(!this.destroyed, 'destroyed');
                this.validateIndexMeta(meta);

                const rawIndex = await M3U8Parse(stream as Stream, { extensions: this.extensions });
                assert(!this.destroyed, 'destroyed');

                (this as any).baseUrl = meta.url;
                (this as any).modified = meta.modified !== null ? new Date(meta.modified) : undefined;
                this.#index = this._preprocessIndex(rawIndex);

                if (this.#index) {
                    this.#playlist = !this.#index.master ? new ParsedPlaylist(MediaPlaylist.cast(this.#index)) : undefined;
                    if (this.#playlist) {
                        if (this.#playlist.index.isLive()) {
                            updated = !fromIndex || !this.#playlist.isSameHead(fromIndex);
                        }

                        updated ||= this.#playlist.index.ended;
                    }

                    if (updated) {
                        const indexMeta: HlsIndexMeta = { url: meta.url, modified: this.modified };
                        process.nextTick(this.emit.bind(this, 'index' as any, this.#index.master ? new MasterPlaylist(this.#index) : new MediaPlaylist(this.#index), indexMeta));
                    }

                    this._updateHints(this.#playlist);

                    // Delay to allow nexttick emits to be delivered

                    await wait();

                    // Update current entry

                    if (this.#playlist && this.#current && !this.#current.isClosed) {
                        const currentSegment = this.#playlist.index.getSegment(this.#current.msn, true);
                        if (currentSegment && (currentSegment.isPartial() || currentSegment.parts)) {
                            this.#current.entry = currentSegment;
                        }
                    }
                }
            }
            catch (err) {
                if (stream) {
                    stream.destroy();
                }

                errored = true;

                if (!this.destroyed) {
                    throw err;
                }
            }
            finally {
                this._stallCheck(updated);

                // Assign #nextUpdate

                if (!this.destroyed && this.#index) {
                    if (this.#playlist?.index.isLive()) {
                        this.#nextUpdate = delayedUpdate(this.#playlist, updated, errored || this.#rejected > 1);
                        this.#nextUpdate.catch((err) => {

                            if (!this.destroyed) {
                                try {
                                    this.emit('problem', err);
                                }
                                catch (err) {
                                    this.destroy(err);
                                }
                            }
                        });
                    }
                    else {
                        this.#nextUpdate = Promise.reject(new Error('Index cannot be updated'));
                    }
                }
                else {
                    this.#nextUpdate = Promise.reject(new Error('Failed to fetch index'));
                }

                assert(this.#nextUpdate);
                this.#nextUpdate.catch(ignore);
            }
        };

        if (this.url.protocol === 'file:') {
            this.#watcher = new FsWatcher(this.url);
        }

        this.#nextUpdate = performUpdate(createFetch(this.url));

        this.#nextUpdate.catch((err) => this.destroyed || this.destroy(err));
    }
}
