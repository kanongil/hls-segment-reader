import type { FetchResult } from './helpers';

import { hrtime } from 'process';
import { finished, Stream } from 'stream';
import { URL } from 'url';
import { promisify } from 'util';

import { Boom } from '@hapi/boom';
import { assert as hoekAssert, ignore, wait } from '@hapi/hoek';
import M3U8Parse, { AttrList, MediaPlaylist, MediaSegment, MasterPlaylist, ParserError } from 'm3u8parse';

import { Byterange, FsWatcher, performFetch } from './helpers';
import { BaseEvents, TypedEmitter } from './raw/typed-readable';


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


export type HlsPlaylistReaderOptions = {
    /** True to handle LL-HLS streams */
    lowLatency?: boolean;

    maxStallTime?: number;

    extensions?: { [K: string]: boolean };
};


export type PartData = {
    uri: string;
    byterange?: Byterange;
};

export type PreloadHints = {
    part?: PartData;
    map?: PartData;
};

export class ParsedPlaylist {

    private _index: Readonly<MediaPlaylist>;

    constructor(index: Readonly<MediaPlaylist>) {

        this._index = index;
    }

    isSameHead(index: Readonly<MediaPlaylist>, includePartial = false): boolean {

        includePartial &&= !this._index.i_frames_only;

        const sameMsn = this._index.lastMsn(includePartial) === index.lastMsn(includePartial);
        if (!sameMsn || !includePartial) {
            return sameMsn;
        }

        // Same + partial check

        return ((this.segments[this.segments.length - 1].parts || []).length ===
            (index.segments[index.segments.length - 1].parts || []).length);
    }

    nextHead(includePartial = false): { msn: number; part?: number } {

        includePartial &&= !this._index.i_frames_only;

        if (includePartial && this.partTarget) {
            const lastSegment = this.segments.length ? this.segments[this.segments.length - 1] : { uri: undefined, parts: undefined };
            const hasPartialSegment = !lastSegment.uri;
            const parts = lastSegment.parts || [];

            return {
                msn: this._index.lastMsn(true) + +!hasPartialSegment,
                part: hasPartialSegment ? parts.length : 0
            };
        }

        return { msn: this._index.lastMsn(false) + 1 };
    }

    get index(): Readonly<MediaPlaylist> {

        return this._index;
    }

    get segments(): readonly Readonly<MediaSegment>[] {

        return this._index.segments;
    }

    get partTarget(): number | undefined {

        const info = this._index.part_info;
        return info ? info.get('part-target', AttrList.Types.Float) || undefined : undefined;
    }

    get serverControl(): { canBlockReload: boolean; partHoldBack?: number } {

        const control = this._index.server_control;
        return {
            canBlockReload: control ? control.get('can-block-reload') === 'YES' : false,
            partHoldBack: control ? control.get('part-hold-back', AttrList.Types.Float) || undefined : undefined
        };
    }

    get preloadHints(): PreloadHints {

        const hints: PreloadHints = {};

        const list = this._index.meta.preload_hints;
        for (const attrs of list || []) {
            const type = attrs.get('type')?.toLowerCase();
            if (attrs.has('uri') && type === 'part' || type === 'map') {
                hints[type] = {
                    uri: attrs.get('uri', AttrList.Types.String) || '',
                    byterange: attrs.has('byterange-start') ? {
                        offset: attrs.get('byterange-start', AttrList.Types.Int),
                        length: (attrs.has('byterange-length') ? attrs.get('byterange-length', AttrList.Types.Int) : undefined)
                    } : undefined
                };
            }
        }

        return hints;
    }
}


export type HlsIndexMeta = {
    url: string;
    modified?: Date;
};

const HlsPlaylistReaderEvents = <IHlsPlaylistReaderEvents & BaseEvents>(null as any);
interface IHlsPlaylistReaderEvents {
    index(index: Readonly<MasterPlaylist>, meta: Readonly<HlsIndexMeta>): void;
    playlist(index: ParsedPlaylist, meta: Readonly<HlsIndexMeta>): void;
    problem(err: Readonly<Error>): void;
}

/**
 * Reads an HLS media playlist, and emits updates.
 * Live & Event playlists are refreshed as needed, and expired segments are dropped when backpressure is applied.
 */
export class HlsPlaylistReader extends TypedEmitter(HlsPlaylistReaderEvents) {

    static readonly recoverableCodes = new Set<number>([
        404, // Not Found
        408, // Request Timeout
        425, // Too Early
        429 // Too Many Requests
    ]);

    readonly url: URL;
    readonly lowLatency: boolean;
    readonly extensions: HlsPlaylistReaderOptions['extensions'];
    stallAfterMs: number;

    readonly baseUrl: string;
    readonly modified?: Date;
    destroyed = false;

    #rejected = 0;
    #indexStalledAt: bigint | null = null;
    #index?: MediaPlaylist | MasterPlaylist;
    #playlist?: ParsedPlaylist;
    #currentHints: ParsedPlaylist['preloadHints'] = {};
    #nextUpdate?: Promise<void>;
    #fetch?: ReturnType<typeof performFetch>;
    #watcher?: FsWatcher;

    constructor(src: string, options: HlsPlaylistReaderOptions = {}) {

        super();

        this.url = new URL(src);
        this.baseUrl = src;

        this.lowLatency = !!options.lowLatency;

        this.stallAfterMs = options.maxStallTime ?? Infinity;

        this.extensions = options.extensions ?? {};

        this._keepIndexUpdated();
    }

    get index(): Readonly<MediaPlaylist | MasterPlaylist> | undefined {

        return this.#index;
    }

    get playlist(): ParsedPlaylist | undefined {

        return this.#playlist;
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

        const { recoverableCodes } = HlsPlaylistReader;

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

    canUpdate(): boolean {

        return !this.index || this.index.isLive();
    }

    /**
     * Resolves once there is an index with a different head, than the passed one.
     *
     * @param index - Current index to compare against
     */
    async waitForUpdate(index?: Readonly<MediaPlaylist>): Promise<ParsedPlaylist> {

        while (!this.destroyed) {
            if (this.index && !this.#playlist) {
                throw new Error('Master playlist cannot be updated');
            }

            if (this.#playlist) {
                const updated = !index || this.#playlist.index.ended || !this.#playlist.isSameHead(index, this.lowLatency);
                if (updated) {
                    return this.#playlist;
                }
            }

            // We need to wait

            await this.#nextUpdate;
        }

        throw new Error('Stream was destroyed');
    }

    destroy(err?: Error | null): void {

        if (this.destroyed) {
            return;
        }

        this.destroyed = true;

        if (this.#fetch) {
            this.#fetch.abort();
        }

        if (this.#watcher) {
            this.#watcher.close();
            this.#watcher = undefined;
        }

        if (err) {
            this.emit<'error'>('error', err);
        }
    }

    // Private methods

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

    protected _preprocessIndex<T extends MediaPlaylist | MasterPlaylist>(index: T): T | undefined {

        // Reject "old" index updates (eg. from CDN cached response & hitting multiple endpoints)

        if (this.index && !this.index.master && this.#rejected < 3) {
            if (MediaPlaylist.cast(index).lastMsn(true) < this.index.lastMsn(true)) {
                this.#rejected++;
                //this.emit<'problem'>('problem', new Error('Rejected update from the past')); // TODO: make recoverable
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

    private _updateHints(playlist: ParsedPlaylist): boolean {

        if (!this.lowLatency) {
            return false;
        }

        const hints = playlist.preloadHints;
        if (internals.isSameHint(hints.part, this.#currentHints.part)) {
            return false;
        }

        this.#currentHints = hints;
        return true;
    }

    private _emitUpdateNextTick(meta: HlsIndexMeta) {

        process.nextTick(() => {

            assert(this.index);

            if (this.index.master) {
                this.emit<'index'>('index', this.index, meta);
            }
            else {
                this.emit<'playlist'>('playlist', this.#playlist!, meta);
            }
        });
    }

    private _updateErrorHandler(err: Error): void {

        if (!this.destroyed) {
            try {
                this.emit<'problem'>('problem', err);
            }
            catch (err) {
                this.destroy(err);
            }
        }
    }

    private _createFetch(url: URL): ReturnType<typeof performFetch> {

        assert(!this.#fetch);

        this.#fetch = performFetch(url, { timeout: 30 * 1000 });

        // Clear this.#fetch when done

        this.#fetch
            .then(({ stream }) => promisify(finished)(stream!))
            .catch(ignore)
            .finally(() => (this.#fetch = undefined));

        return this.#fetch;
    }

    /**
     * Call once to fetch index and start update loop (if needed).
     */
    private _keepIndexUpdated() {

        assert(!this.#nextUpdate, 'Already called');

        // Prepare watcher in case it is needed

        if (this.url.protocol === 'file:') {
            this.#watcher = new FsWatcher(this.url);
        }

        // Create initial fetch, and start update loop

        this.#nextUpdate = this._performUpdate(this._createFetch(this.url));

        // If the initial fetch fails, hard error

        this.#nextUpdate.catch(this.destroy.bind(this));
    }

    /**
     * Runs in a loop until there are no more updates, or stream is destroyed
     */
    private async _performUpdate(fetchPromise: ReturnType<typeof performFetch>, fromIndex?: Readonly<MediaPlaylist>): Promise<void> {

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

            if (this.index) {
                updated ||= !this.canUpdate();      // If there are no more updates, then we need to signal the index

                this.#playlist = !this.index.master ? new ParsedPlaylist(this.index) : undefined;
                if (this.#playlist) {
                    if (fromIndex && this.canUpdate()) {
                        updated ||= !this.#playlist.isSameHead(fromIndex);
                    }

                    updated = this._updateHints(this.#playlist) || updated; // No ||= since the update has a side-effect, and will not be called if updated is already set
                }

                if (updated) {
                    this._emitUpdateNextTick({ url: meta.url, modified: this.modified });

                    await wait(0);      // wait until nexttick emit has been resolved
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
                if (this.#playlist && this.canUpdate()) {
                    this.#nextUpdate = this._delayedUpdate(this.#playlist, updated, errored || this.#rejected > 1);
                    this.#nextUpdate.catch(this._updateErrorHandler.bind(this));
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

    /**
     * Calls _performUpdate() with corrected url, after an approriate delay
     */
    private async _delayedUpdate(fromPlaylist: ParsedPlaylist, wasUpdated: boolean, wasError = false): ReturnType<HlsPlaylistReader['_performUpdate']> {

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
                if (!this.destroyed) {
                    this.emit<'problem'>('problem', err);
                }

                this.#watcher = undefined;
            }
        }
        else {
            await wait(delayMs);
        }

        assert(!this.destroyed, 'destroyed');

        return await this._performUpdate(this._createFetch(url), fromPlaylist.index);
    }
}
