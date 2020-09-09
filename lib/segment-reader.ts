import type { Stream } from 'stream';
import type { MediaPlaylist, M3U8IndependentSegment } from 'm3u8parse/lib/m3u8playlist';
import type { FetchResult } from './helpers';

import { hrtime } from 'process';
import { URL } from 'url';

import { Boom } from '@hapi/boom';
import { assert as hoekAssert, ignore, wait } from '@hapi/hoek';
import { AttrList } from 'm3u8parse/lib/attrlist';
import M3U8Parse, { ParserError } from 'm3u8parse/lib/m3u8parse';
import { M3U8Playlist } from 'm3u8parse/lib/m3u8playlist'; // TODO: import from m3u8parse
import { Readable } from 'readable-stream';

import { ParsedPlaylist, FsWatcher, performFetch } from './helpers';


// eslint-disable-next-line func-style
function assert(condition: any, ...args: any[]): asserts condition {

    hoekAssert(condition, ...args);
}


type Hint = {
    uri: string,
    byterange?: {
        offset: number,
        length?: number
    }
};


const internals = {
    indexMimeTypes: new Set([
        'application/vnd.apple.mpegurl',
        'application/x-mpegurl',
        'audio/mpegurl'
    ]),

    isSameHint(h1: Hint, h2: Hint): boolean {

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

    onUpdate?: ((entry: M3U8IndependentSegment, old?: M3U8IndependentSegment) => void);

    #entry: M3U8IndependentSegment;
    #closed = false;

    constructor(msn: number, segment: M3U8IndependentSegment) {

        this.msn = msn;
        this.#entry = segment;
    }

    get entry(): M3U8IndependentSegment {

        return this.#entry;
    }

    set entry(entry: M3U8IndependentSegment) {

        assert(!this.closed);

        const old = this.#entry;
        this.#entry = entry;

        entry.discontinuity = !!(+entry.discontinuity | +old.discontinuity);

        this.#closed = !entry.isPartial();

        if (this.onUpdate) {
            process.nextTick(this.onUpdate.bind(this, entry, old));
        }
    }

    get closed(): boolean {

        return this.#closed;
    }

    _abandon(): void {

        if (!this.#closed && this.onUpdate) {
            process.nextTick(this.onUpdate.bind(this, this.#entry));
        }

        this.#closed = true;
    }
}

export type HlsSegmentReaderOptions = {
    /** Start from first segment, or use stream start */
    fullStream?: boolean;

    /** True to handle LL-HLS streams */
    lowLatency?: boolean;

    startDate?: Date;
    stopDate?: Date;

    maxStallTime?: number;

    extensions?: { [K: string]: boolean }
};


import { TypedReadable, ReadableEvents } from './raw/typed-readable';

interface HlsSegmentReaderEvents extends ReadableEvents<HlsReaderObject> {
    index: (index: M3U8Playlist) => void;
}


/**
 * Reads an HLS media playlist, and output segments in order.
 * Live & Event playlists are refreshed as needed, and expired segments are dropped when backpressure is applied.
 */
export class HlsSegmentReader extends TypedReadable<HlsReaderObject, HlsSegmentReaderEvents> {

    static readonly recoverableCodes = new Set<number>([
        404, // Not Found
        408, // Request Timeout
        425, // Too Early
        429 // Too Many Requests
    ]);

    readonly url: URL;
    baseUrl: string;
    readonly fullStream: boolean;
    readonly lowLatency: boolean;
    readonly startDate: Date | null;
    readonly stopDate: Date | null;
    readonly stallAfterMs: number;
    readonly extensions: HlsSegmentReaderOptions['extensions'];

    #next = new SegmentPointer();
    #discont = false;
    #indexStalledAt: bigint | null = null;
    #index?: M3U8Playlist;
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

        this.startDate = options.startDate ? new Date(options.startDate) : null;
        this.stopDate = options.stopDate ? new Date(options.stopDate) : null;

        this.stallAfterMs = options.maxStallTime ?? Infinity;

        this.extensions = options.extensions ?? {};

        this._keepIndexUpdated();
    }

    get index(): M3U8Playlist | undefined {

        return this.#index;
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
    isRecoverableUpdateError(err: Error) {

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
     * Called to push the next segment.
     */
    /*protected*/ async _read(): Promise<void> {

        try {
            let ready = true;
            while (ready) {
                try {
                    const last = this.#current;
                    this.#current = await this._getNextSegment();
                    last?._abandon();
                    ready = this.push(this.#current);
                }
                catch (err) {
                    if (this.index) {
                        if (this.index.master) {
                            this.push(null);                    // Just ignore any error
                            return;
                        }

                        if (this.isRecoverableUpdateError(err)) {
                            this.emit('problem', err);
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

    /*protected*/ _destroy(err: Error | null, cb: any): void {

        super._destroy(err, cb);

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
            else if ((this.#next.msn < playlist.startMsn(true)) ||
                     (this.#next.msn > (playlist.lastMsn(this.lowLatency) + 1))) {

                // Playlist jump

                if (playlist.index.type /* VOD or event */) {
                    throw new Error('Fatal playlist inconsistency');
                }

                this.#next = new SegmentPointer(playlist.startMsn(true), this.lowLatency ? 0 : undefined);
                this.#discont = true;
            }

            const next = this.#next;
            const segment = playlist.getResolvedSegment(next.msn, this.baseUrl);
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

    /**
     * Resolves once there is an index with a different head, than the passed one.
     */
    private async _waitForUpdate(playlist?: ParsedPlaylist): Promise<ParsedPlaylist | undefined> {

        while (!this.destroyed) {
            if (this.index && !this.#playlist) {
                throw new Error('Master playlist cannot be updated');
            }

            if (this.#playlist) {
                const updated = !playlist || this.#playlist.index.ended || !this.#playlist.isSameHead(playlist.index, this.lowLatency);
                this._stallCheck(updated);
                if (updated) {
                    return this.#playlist;
                }
            }

            // We need to wait

            await this.#nextUpdate;
        }
    }

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

        let updateInterval = index.target_duration;
        if (this.lowLatency && partTarget! > 0) {
            updateInterval = partTarget;
        }

        if (updateInterval === undefined) {
            updateInterval = Number.NaN;
        }

        if (!updated || !index.segments.length) {
            updateInterval /= 2;
        }

        return updateInterval;
    }

    private _initialSegmentPointer(playlist: ParsedPlaylist): SegmentPointer {

        if (!this.fullStream && this.startDate) {
            const msn = playlist.msnForDate(this.startDate);
            if (msn >= 0) {
                return new SegmentPointer(msn, this.lowLatency ? 0 : undefined);
            }

            // no date information in index
        }

        if (this.lowLatency && playlist.serverControl.partHoldBack) {
            let partHoldBack = playlist.serverControl.partHoldBack;
            let msn = playlist.lastMsn(true);
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

        return new SegmentPointer(playlist.startMsn(this.fullStream), this.lowLatency ? 0 : undefined);
    }

    protected _preprocessIndex(index: M3U8Playlist): M3U8Playlist | undefined {

/*        if (!this.lowLatency) {

            // Ignore partial-only segment

            if (index.segments.length && !index.segments[index.segments.length - 1].uri) {
                index.segments.pop();
            }

            // TODO: strip all low-latency
        }*/

        return index;
    }

    private _emitHintsNT(playlist?: ParsedPlaylist) {

        if (!this.lowLatency || !playlist || !playlist.serverControl.canBlockReload) {
            return;               // Server does not support blocking
        }

        const hints = playlist.preloadHints;
        for (const type of (['map', 'part'] as (keyof ParsedPlaylist['preloadHints'])[])) {
            const hint = hints[type];
            const last = this.#currentHints[type];

            if (hint && (!last || !internals.isSameHint(hint, last))) {
                this.#currentHints[type] = hint;
                process.nextTick(this.emit.bind(this, 'hint', { ...hint, type }));
            }
        }
    }

    private _keepIndexUpdated() {

        assert(!this.#nextUpdate);

        const createFetch = (url: URL) => {

            return (this.#fetch = performFetch(url, { timeout: 30 * 1000 }));
        };

        const delayedUpdate = async (fromPlaylist: ParsedPlaylist, wasUpdated: boolean, wasError = false) => {

            let delay = this._getUpdateInterval(fromPlaylist, wasUpdated && !wasError);

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

                delay = 0;
            }

            if (delay && this.#watcher) {
                try {
                    await Promise.race([wait(delay), this.#watcher.next()]);
                }
                catch (err) {
                    this.emit('problem', err);
                    this.#watcher = undefined;
                }
            }
            else {
                await wait(delay);
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
                this.baseUrl = meta.url;

                const rawIndex = await M3U8Parse(stream as Stream, { extensions: this.extensions });
                assert(!this.destroyed, 'destroyed');

                this.#index = this._preprocessIndex(rawIndex);

                if (this.#index) {
                    this.#playlist = !this.#index.master ? new ParsedPlaylist(M3U8Playlist.castAsMedia(this.#index)) : undefined;
                    if (this.#playlist?.index.isLive()) {
                        updated = !fromIndex || this.#playlist.index.ended || !this.#playlist.isSameHead(fromIndex);
                    }

                    if (updated) {
                        process.nextTick(this.emit.bind(this, 'index', new M3U8Playlist(this.#index)));
                    }

                    this._emitHintsNT(this.#playlist);

                    // Delay to allow nexttick emits to be delivered

                    await wait();

                    // Update current entry

                    if (this.#playlist && this.#current?.entry.isPartial()) {
                        const currentSegment = this.#playlist.getResolvedSegment(this.#current.msn, this.baseUrl);
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
                // Assign #nextUpdate

                if (!this.destroyed && this.#index) {
                    if (this.#playlist?.index.isLive()) {
                        this.#nextUpdate = delayedUpdate(this.#playlist, updated, errored);
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
    }
}
