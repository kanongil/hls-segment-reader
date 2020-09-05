'use strict';

/** @typedef { import('m3u8parse/lib/m3u8playlist').MediaPlaylist } MediaPlaylist */
/** @typedef { import('m3u8parse/lib/m3u8playlist').M3U8IndependentSegment } M3U8IndependentSegment */

const Process = require('process');
const Url = require('url');

const Boom = require('@hapi/boom');
const Hoek = require('@hapi/hoek');
const M3U8Parse = require('m3u8parse');
const { M3U8Playlist, AttrList, ParserError } = require('m3u8parse');
const Readable = require('readable-stream');

const Helpers = require('./helpers');


/**
 * @param {any} condition
 * @param {any[]} args
 * @return {asserts condition}
 */
// eslint-disable-next-line func-style
function assert(condition, ...args) {

    Hoek.assert(condition, ...args);
}


const internals = {
    indexMimeTypes: new Set([
        'application/vnd.apple.mpegurl',
        'application/x-mpegurl',
        'audio/mpegurl'
    ]),

    /**
     * @param {{ uri: string, byterange?: { offset: number, length?: number } }} h1
     * @param {{ uri: string, byterange?: { offset: number, length?: number } }} h2
     */
    isSameHint(h1, h2) {

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


const SegmentPointer = class {

    /**
     * @param {number} msn
     * @param {number | undefined} part
     */
    constructor(msn = -1, part = undefined) {

        this.msn = +msn;
        this.part = part;
    }

    next() {

        return new SegmentPointer(this.msn + 1, this.part === undefined ? undefined : 0);
    }

    isEmpty() {

        return this.msn < 0;
    }
};


exports.HlsReaderObject = class HlsReaderObject {

    /** @type {((entry: M3U8IndependentSegment, old?: M3U8IndependentSegment) => void) | undefined} */
    onUpdate;

    /** @type {M3U8IndependentSegment} */
    #entry;

    #closed = false;

    /**
     * @param {number} msn
     * @param {M3U8IndependentSegment} segment
     */
    constructor(msn, segment) {

        this.msn = msn;
        this.#entry = segment;
    }

    get entry() {

        return this.#entry;
    }

    /**
     * @param {M3U8IndependentSegment} entry
     */
    set entry(entry) {

        assert(!this.closed);

        const old = this.#entry;
        this.#entry = entry;

        entry.discontinuity = !!(+entry.discontinuity | +old.discontinuity);

        this.#closed = !entry.isPartial();

        if (this.onUpdate) {
            process.nextTick(this.onUpdate.bind(this, entry, old));
        }
    }

    get closed() {

        return this.#closed;
    }

    _abandon() {

        if (!this.#closed && this.onUpdate) {
            process.nextTick(this.onUpdate.bind(this, this.#entry));
        }

        this.#closed = true;
    }
};


/**
 * @typedef HlsSegmentReaderOptions
 * @type {object}
 * @property {boolean} [fullStream=false] - Start from first segment, or use stream start
 * @property {boolean} [lowLatency=false] - True to handle LL-HLS streams
 * @property {Date} [startDate]
 * @property {Date} [stopDate]
 * @property {number} [maxStallTime=Infinity]
 * @property {{ [K: string]: boolean }} [extensions]
 */

/**
 * Reads an HLS media playlist, and output segments in order.
 * Live & Event playlists are refreshed as needed, and expired segments are dropped when backpressure is applied.
 */
exports.HlsSegmentReader = class HlsSegmentReader extends Readable {

    static recoverableCodes = new Set([
        404, // Not Found
        408, // Request Timeout
        425, // Too Early
        429 // Too Many Requests
    ]);

    #next = new SegmentPointer();
    #discont = false;

    /** @type {bigint | null} */
    #indexStalledAt = null;

    /** @type {M3U8Playlist | undefined} */
    #index;

    /** @type {Helpers.ParsedPlaylist | undefined} */
    #playlist;

    /** @type {?exports.HlsReaderObject} */
    #current = null;

    /** @type {Helpers.ParsedPlaylist['preloadHints']} */
    #currentHints = {};

    /** @type {Promise<void> | undefined} */
    #nextUpdate;

    /** @type {ReturnType<Helpers.fetch> | undefined} */
    #fetch;

    /** @type {Helpers.FsWatcher | undefined}  */
    #watcher;

    /**
     * @param {string} src
     * @param {HlsSegmentReaderOptions} [options]
     */
    constructor(src, options = {}) {

        super({ objectMode: true, highWaterMark: 0 });

        this.url = new Url.URL(src);
        this.baseUrl = src;

        this.fullStream = !!options.fullStream;
        this.lowLatency = !!options.lowLatency;

        // dates are inclusive
        this.startDate = options.startDate ? new Date(options.startDate) : null;
        this.stopDate = options.stopDate ? new Date(options.stopDate) : null;

        this.stallAfterMs = options.maxStallTime || Infinity;

        this.extensions = options.extensions || {};

        this._keepIndexUpdated();
    }

    get index() {

        return this.#index;
    }

    get indexMimeTypes() {

        return internals.indexMimeTypes;
    }

    /**
     * @param {Helpers.FetchResult['meta']} meta
     */
    validateIndexMeta(meta) {

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
     *
     * @param {Error} err
     */
    isRecoverableUpdateError(err) {

        const { recoverableCodes } = HlsSegmentReader;

        if (err instanceof Boom.Boom) {
            if (err.isServer || recoverableCodes.has(err.output.statusCode)) {
                return true;
            }
        }

        if (err instanceof ParserError) {
            return true;
        }

        if (/** @type {*} */ (err).syscall) {      // Any syscall error
            return true;
        }

        return false;
    }

    /**
     * Called to push the next segment.
     */
    async _read() {

        try {
            let ready = true;
            while (ready) {
                try {
                    const last = this.#current;
                    this.#current = await this._getNextSegment();
                    if (last) {
                        last._abandon();
                    }

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

    /**
     * @param {?Error} err
     * @param {*} cb
     */
    _destroy(err, cb) {

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

    /**
     * @return {Promise<exports.HlsReaderObject | null>}
     */
    async _getNextSegment() {

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
                (next.part && next.part >= (segment.parts || []).length)) {

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

            return new exports.HlsReaderObject(next.msn, segment);
        }

        return null;
    }

    /**
     * Resolves once there is an index with a different head, than the passed one.
     *
     * @param {Helpers.ParsedPlaylist} [playlist]
     *
     * @return {Promise<Helpers.ParsedPlaylist | undefined>}
     */
    async _waitForUpdate(playlist) {

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

    _stallCheck(updated = false) {

        if (updated) {
            this.#indexStalledAt = null;
        }
        else {
            if (this.#indexStalledAt === null) {
                this.#indexStalledAt = Process.hrtime.bigint();      // Record when stall began
            }
            else {
                if (Number((Process.hrtime.bigint() - this.#indexStalledAt) / BigInt(1000000)) > this.stallAfterMs) {
                    throw new Error('Index update stalled');
                }
            }
        }
    }

    /**
     * @param {Helpers.ParsedPlaylist} playlist
     * @return {number}
     */
    _getUpdateInterval({ index, partTarget }, updated = false) {

        let updateInterval = index.target_duration;
        if (this.lowLatency && partTarget && partTarget > 0) {
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

    /**
     * @param {Helpers.ParsedPlaylist} playlist
     */
    _initialSegmentPointer(playlist) {

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

    /**
     * @param {M3U8Playlist} index
     *
     * @return {M3U8Playlist | undefined}
     */
    _preprocessIndex(index) {

/*        if (!this.lowLatency) {

            // Ignore partial-only segment

            if (index.segments.length && !index.segments[index.segments.length - 1].uri) {
                index.segments.pop();
            }

            // TODO: strip all low-latency
        }*/

        return index;
    }

    /**
     * @param {Helpers.ParsedPlaylist | undefined} playlist
     */
    _emitHintsNT(playlist) {

        if (!this.lowLatency || !playlist || !playlist.serverControl.canBlockReload) {
            return;               // Server does not support blocking
        }

        const hints = playlist.preloadHints;
        for (const type of /** @type {(keyof Helpers.ParsedPlaylist['preloadHints'])[]} */(['map', 'part'])) {
            const hint = hints[type];
            const last = this.#currentHints[type];

            if (hint && (!last || !internals.isSameHint(hint, last))) {
                this.#currentHints[type] = hint;
                process.nextTick(this.emit.bind(this, 'hint', { ...hint, type }));
            }
        }
    }

    _keepIndexUpdated() {

        assert(!this.#nextUpdate);

        /**
         * @param {Url.URL} url
         */
        const createFetch = (url) => {

            return (this.#fetch = Helpers.fetch(url.href, { timeout: 30 * 1000 }));
        };

        /**
         * @param {Helpers.ParsedPlaylist} fromPlaylist
         * @param {boolean} wasUpdated
         */
        const delayedUpdate = async (fromPlaylist, wasUpdated, wasError = false) => {

            let delay = this._getUpdateInterval(fromPlaylist, wasUpdated && !wasError);

            const url = new Url.URL(/** @type {any} */(this.url));
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
                    await Promise.race([Hoek.wait(delay), this.#watcher.next()]);
                }
                catch (err) {
                    this.emit('problem', err);
                    this.#watcher = undefined;
                }
            }
            else {
                await Hoek.wait(delay);
            }

            assert(!this.destroyed, 'destroyed');

            await performUpdate(createFetch(url), fromPlaylist.index);
        };

        /**
         * Runs in a loop until there are no more updates, or stream is destroyed
         *
         * @param {ReturnType<Helpers.fetch>} fetch
         * @param {MediaPlaylist} [fromIndex]
         *
         * @return {Promise<void>}
         */
        const performUpdate = async (fetch, fromIndex) => {

            let updated = !fromIndex;
            let errored = false;
            try {
                var { meta, stream } = await fetch;

                assert(!this.destroyed, 'destroyed');
                this.validateIndexMeta(meta);
                this.baseUrl = meta.url;

                const rawIndex = await M3U8Parse(stream, { extensions: this.extensions });
                assert(!this.destroyed, 'destroyed');

                this.#index = this._preprocessIndex(rawIndex);

                if (this.#index) {
                    this.#playlist = !this.#index.master ? new Helpers.ParsedPlaylist(M3U8Playlist.castAsMedia(this.#index)) : undefined;
                    if (this.#playlist && this.#playlist.index.isLive()) {
                        updated = !fromIndex || this.#playlist.index.ended || !this.#playlist.isSameHead(fromIndex);
                    }

                    if (updated) {
                        process.nextTick(this.emit.bind(this, 'index', new M3U8Playlist(this.#index)));
                    }

                    this._emitHintsNT(this.#playlist);

                    // Delay to allow nexttick emits to be delivered

                    await Hoek.wait();

                    // Update current entry

                    if (this.#playlist && this.#current && this.#current.entry.isPartial()) {
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
                    if (this.#playlist && this.#playlist.index.isLive()) {
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
                this.#nextUpdate.catch(Hoek.ignore);
            }
        };

        const url = new Url.URL(/** @type {any} */(this.url));
        if (url.protocol === 'file:') {
            this.#watcher = new Helpers.FsWatcher(url);
        }

        this.#nextUpdate = performUpdate(createFetch(url));
    }
};
