'use strict';

const Stream = require('stream');
const Url = require('url');

const Hoek = require('@hapi/hoek');
const M3U8Parse = require('m3u8parse');
const Readable = require('readable-stream');

const Helpers = require('./helpers');
const SegmentFetcher = require('./segment-fetcher');
const HlsSegmentObject = require('./segment-object');
const { M3U8Playlist, AttrList, M3U8Segment, IMediaPlaylist, M3U8IndependentSegment } = require('m3u8parse');

try {
    const MimeTypes = require('mime-types');

    /* eslint-disable dot-notation */
    MimeTypes.types['ac3'] = 'audio/ac3';
    MimeTypes.types['eac3'] = 'audio/eac3';
    MimeTypes.types['m4s'] = 'video/iso.segment';
    /* eslint-enable dot-notation */
}
catch (err) {
    console.error('Failed to inject extra types', err);
}

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
    segmentMimeTypes: new Set([
        'video/mp2t',
        'video/mpeg',
        'video/mp4',
        'video/iso.segment',
        'video/x-m4v',
        'audio/aac',
        'audio/x-aac',
        'audio/ac3',
        'audio/vnd.dolby.dd-raw',
        'audio/x-ac3',
        'audio/eac3',
        'audio/mp4',
        'text/vtt',
        'application/mp4'
    ]),

    /**
     * @param {AttrList} [m1]
     * @param {AttrList} [m2]
     */
    isSameMap(m1, m2) {

        return m1 && m2 && m1.get('uri') === m2.get('uri') && m1.get('byterange') === m2.get('byterange');
    },

    /**
     * @param {IMediaPlaylist} index
     */
    nextMsn(index) {

        const hasPartialSegment = (index.segments.length && !index.segments[index.segments.length - 1].uri);
        return index.lastSeqNo() + ~~!hasPartialSegment;
    }
};


const SegmentPointer = class {

    /**
     * @param {number} msn
     * @param {number | undefined} part
     */
    constructor(msn = -1, part = undefined, map = false) {

        if (typeof part === 'boolean') {
            map = part;
            part = undefined;
        }

        this.msn = +msn;
        this.part = part;
        this.isMap = !!map;
    }

    /**
     * @param {M3U8Segment | M3U8IndependentSegment} [segment]
     */
    next(segment, ended = false) {

        if (this.part !== undefined && !ended) {
            if (segment && !segment.uri) {
                return new SegmentPointer(this.msn, segment.parts && segment.parts.length);
            }
        }

        return new SegmentPointer(this.msn + 1, this.part === undefined ? undefined : 0);
    }

    toMap() {

        return new SegmentPointer(this.msn, this.part, true);
    }

    isEmpty() {

        return this.msn < 0;
    }

    /**
     * Returns whether it points to the segment (and part), just after the last in the index.
     *
     * @param {IMediaPlaylist} index
     */
    isHead(index) {

        if (index.isValidSeqNo(this.msn, this.part)) {
            return false;
        }

        if (this.part === undefined) {
            return index.isValidSeqNo(this.msn - 1);
        }

        if (this.part === 0) {
            return index.lastSeqNo(false) === this.msn - 1;
        }

        return index.isValidSeqNo(this.msn, this.part - 1);
    }
};


/**
 * @typedef HlsSegmentReaderOptions
 * @type {object}
 * @property {number} [highWaterMark=0]
 * @property {boolean} [fullStream=false]
 * @property {boolean} [withData=false]
 * @property {boolean} [lowLatency=false]
 * @property {Date} [startDate]
 * @property {Date} [stopDate]
 * @property {number} [maxStallTime=Infinity]
 * @property {{ [K: string]: boolean }} [extensions]
 */

exports = module.exports = class HlsSegmentReader extends Readable {

    #readState = {
        current: new SegmentPointer(),

        next: new SegmentPointer(),
        mapSeq: -1,
        /** @type {AttrList | undefined} */
        map: undefined,
        /** @type {?Promise<HlsSegmentObject>} */
        fetching: null,
        active: false,
        discont: false
    };

    /** @type {Map<number,HlsSegmentObject & { stream?: NodeJS.ReadStream & { addParts?: function, addHint?: function }}>} */
    #active = new Map(); // used to stop buffering on expired segments

    /** @type {SegmentFetcher} */
    #fetcher;

    /** @type {number | null} */
    #indexStallSince = null;

    /** @type {ReturnType<setTimeout> | undefined} */
    #updateTimer;

    /** @type {M3U8Playlist | undefined} */
    #index;

    /**
     * @param {string} src
     * @param {HlsSegmentReaderOptions} options
     */
    constructor(src, options = {}) {

        super({ objectMode: true, highWaterMark: options.highWaterMark || 0 });

        this.url = new Url.URL(src);
        this.baseUrl = src;

        this.fullStream = !!options.fullStream;
        this.withData = !!options.withData;
        this.lowLatency = !!options.lowLatency;

        // dates are inclusive
        this.startDate = options.startDate ? new Date(options.startDate) : null;
        this.stopDate = options.stopDate ? new Date(options.stopDate) : null;

        this.maxStallTime = options.maxStallTime || Infinity;

        this.extensions = options.extensions || {};

        this.#fetcher = new SegmentFetcher({ probe: !this.withData });

        this._updateindex();
    }

    abort(graceful = false) {

        // @ts-ignore
        clearTimeout(this.#updateTimer);

        if (!graceful) {
            this.#fetcher.abort();
        }

        if (!this.readable) {
            return;
        }

        if (!this._readableState.ended) {
            this.push(null);
        }

        this.readable = false;
    }

    /**
     * @param {?Error} err
     * @param {*} cb
     */
    _destroy(err, cb) {

        this.abort(!!err);

        return super._destroy(err, cb);
    }

    get index() {

        return this.#index;
    }

    get indexMimeTypes() {

        return internals.indexMimeTypes;
    }

    get segmentMimeTypes() {

        return internals.segmentMimeTypes;
    }

    /**
     * @param {Helpers.FetchResult['meta']} meta
     */
    validateIndexMeta(meta) {

        // Check for valid mime type

        if (!this.indexMimeTypes.has(meta.mime.toLowerCase()) &&
            meta.url.indexOf('.m3u8', meta.url.length - 5) === -1 &&
            meta.url.indexOf('.m3u', meta.url.length - 4) === -1) {

            throw new Error('Invalid MIME type: ' + meta.mime);
        }
    }

    /**
     * @param {Helpers.FetchResult['meta']} meta
     */
    validateSegmentMeta(meta) {

        // Check for valid mime type

        if (!this.segmentMimeTypes.has(meta.mime.toLowerCase())) {
            throw new Error(`Unsupported segment MIME type: ${meta.mime}`);
        }
    }

    _read(/*n*/) {

        this.#readState.active = true;
        this._checkNext();
    }

    // Private methods

    /**
     * @return {void}
     */
    _checkNext() {

        const state = this.#readState;
//        console.trace('_checkNext', !!this.readable, !!state.active, !state.fetching, !state.next.isEmpty(), !!index)
        if (!this.readable || !state.active || state.fetching || state.next.isEmpty() || !this.index) {
            return;
        }

        let { next } = state;
        const index = M3U8Playlist.castAsMedia(this.index);
        const segment = index.getSegment(next.msn, true);

        // Handle low latency hints

        if (this.lowLatency && index.meta.preload_hints && index.server_control) {
            const canBlock = index.server_control.get('can-block-reload') === 'YES';
            if (canBlock && next.isHead(index)) {
                const active = this.#active.get(next.msn);
                if (active && active.stream && active.stream.addHint) {
                    for (const hintAttrs of index.meta.preload_hints) {
                        const hint = {
                            uri: hintAttrs.get('uri', AttrList.Types.String),
                            type: hintAttrs.get('type'),
                            byterange: hintAttrs.has('byterange-start') ? {
                                offset: hintAttrs.get('byterange-start', AttrList.Types.Int),
                                length: (hintAttrs.has('byterange-length') ? hintAttrs.get('byterange-length', AttrList.Types.Int) : undefined)
                            } : undefined
                        };

                        active.stream.addHint(hint);
                    }
                }

                // TODO: hint when no active stream
            }
        }

        if (segment && (this.lowLatency || segment.uri)) {
            // mark manual discontinuities
            if (state.discont) {
                segment.discontinuity = true;
                state.discont = false;
                state.map = undefined;
            }

            // Check if we need to stop

            if (this.stopDate && (segment.program_time || 0) > this.stopDate) {
                this.push(null);
                return;
            }

            // TODO: close part stream on jumps

            if (next.part !== undefined) {

                // Check if there is an active segment to append to

                const active = this.#active.get(next.msn);
                if (active) {
                    assert(segment.parts);
                    const segmentParts = segment.parts.slice(next.part);
                    const final = !!segment.uri || index.ended;

                    if (segmentParts.length === 0 && !final) {
                        return;
                    }

                    const parts = segmentParts.map((part) => ({
                        uri: Url.resolve(this.baseUrl, part.get('uri', AttrList.Types.String) || ''),
                        byterange: part.get('byterange', AttrList.Types.Byterange)
                    }));

                    active.stream.addParts(parts, final);
                    active.segment.details.parts.push(...segmentParts); // TODO: signal that more parts were added

                    state.next = next.next(segment, index.ended);
                    state.active = !final;

                    return this._checkNext();
                }
            }

            if (segment.map) {
                if (internals.isSameMap(segment.map, state.map)) {
                    delete segment.map;
                }
                else {
                    next = next.toMap();
                }
            }

            // TODO: fetch hint, if at edge

            state.fetching = this._fetchFrom(next, segment);
            state.fetching.finally(() => {

                state.fetching = null;
            }).then((object) => {

                if (!this.readable) {
                    return;
                }

                // Advance, if still current

                if (next === state.next) {
                    state.next = next.next(object.segment ? object.segment.details : undefined, index.ended);
                }

                if (object.file.modified) {
                    const segmentTime = segment.program_time || new Date(+object.file.modified - (segment.duration || 0) * 1000);
                    if (this.startDate && segmentTime < this.startDate) {
                        // too early - drop segment
                        if (object.stream) {
                            object.stream.destroy();
                        }

                        return this._checkNext();
                    }

                    if (this.stopDate && segmentTime > this.stopDate) {
                        // check that this is also valid for next segment with date
                        if (object.stream) {
                            object.stream.destroy();
                        }

                        return this.push(null);
                    }
                }

                if (object.stream) {
                    this.#active.set(next.msn, object);
                    Stream.finished(object.stream, () => this.#active.delete(next.msn));
                }

                if (next.isMap) {
                    state.map = segment.map;
                }

                state.active = this.push(object) || next.msn === state.next.msn;

                this._checkNext();
            }).catch(this.emit.bind(this, 'error'));
        }
        else if (index.ended) {
            this.push(null);
        }
        else if (!index.type && (index.lastSeqNo() < state.next.msn - 1)) {
            // handle live stream restart
            state.discont = true;
            state.next = new SegmentPointer(index.startSeqNo(true), this.lowLatency ? 0 : undefined);
            this._checkNext();
        }
    }

    /**
     * @param {SegmentPointer} ptr
     * @param {M3U8IndependentSegment} segment
     */
    async _fetchFrom(ptr, segment) {

        let uri = segment.uri;
        let byterange = segment.byterange;

        if (ptr.isMap) {
            assert(segment.map);

            // Fetch init segment

            uri = segment.map.get('uri', AttrList.Types.String);
            assert(uri, 'EXT-X-MAP must have URI attribute');
            if (segment.map.has('byterange')) {
                // Byterange in map is _not_ byterange encoded - rather it is a quoted string!

                const [length, offset = '0'] = (segment.map.get('byterange', AttrList.Types.String) || '').split('@');
                byterange = {
                    offset: parseInt(offset, 10),
                    length: parseInt(length, 10)
                };
            }
            else {
                byterange = undefined;
            }
        }

        let fetch;
        try {
            if (uri === undefined || ptr.part) {

                // Create part request

                assert(segment.parts);
                const parts = segment.parts.slice(ptr.part || 0).map((part) => ({
                    uri: Url.resolve(this.baseUrl, part.get('uri', AttrList.Types.String) || ''),
                    /** @type {Required<Helpers.Byterange> | undefined} */
                    byterange: part.has('byterange') ? Object.assign({ offset: 0 }, part.get('byterange', AttrList.Types.Byterange)) : undefined
                }));
                fetch = await this.#fetcher.fetchParts(parts, !!segment.uri);
            }
            else {
                fetch = await this.#fetcher.fetchSegment(Url.resolve(this.baseUrl, uri), byterange);
                segment.parts = undefined;
            }

            this.validateSegmentMeta(fetch.meta);

            return new HlsSegmentObject(fetch.meta, fetch.stream, ptr, ptr.isMap && segment.map ? segment.map : segment);
        }
        catch (err) {
            if (fetch && fetch.stream) {
                fetch.stream.destroy();
            }

            throw err;
        }
    }

    _stallCheck(updated = false) {

        if (updated) {
            this.#indexStallSince = null;
        }
        else {
            if (this.#indexStallSince === null) {
                this.#indexStallSince = Date.now();      // Record when stall began
            }
            else {
                if ((Date.now() - this.#indexStallSince) > this.maxStallTime) {
                    throw new Error('Index update stalled');
                }
            }
        }
    }

    /**
     * @param {IMediaPlaylist} index
     * @return {number}
     */
    _getUpdateInterval(index, updated = false) {

        let updateInterval = index.target_duration;
        if (this.lowLatency) {
            const partTarget = index.part_info && index.part_info.get('part-target', AttrList.Types.Float);
            if (partTarget && partTarget > 0) {
                updateInterval = partTarget;
            }
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
     * @param {IMediaPlaylist} index
     */
    _initialSegmentPointer(index) {

        // TODO: update with parts

        if (!this.fullStream && this.startDate) {
            const seqNo = index.seqNoForDate(this.startDate, true);
            if (seqNo >= 0) {
                return new SegmentPointer(seqNo, this.lowLatency ? 0 : undefined);
            }

            // no date information in index - it will be approximated from segment metadata
        }

        if (this.lowLatency && index.server_control && index.server_control.has('part-hold-back')) {
            let partHoldBack = index.server_control.get('part-hold-back', AttrList.Types.Float);
            let msn = index.lastSeqNo();
            let segment;
            while (segment = index.getSegment(msn)) {
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

        return new SegmentPointer(index.startSeqNo(this.fullStream), this.lowLatency ? 0 : undefined);
    }

    _updateindex() {

        /**
         * @param {ReturnType<Helpers.fetch>} promise
         */
        const perform = async (promise) => {

            let updated = false;
            try {
                var { meta, stream } = await promise;

                assert(this.readable);
                this.validateIndexMeta(meta);
                this.baseUrl = meta.url;

                const index = await M3U8Parse(stream, { extensions: this.extensions });
                assert(this.readable);

/*                if (!this.lowLatency) {

                    // Ignore partial-only segment

                    if (index.segments.length && !index.segments[index.segments.length - 1].uri) {
                        index.segments.pop();
                    }

                    // TODO: strip all low-latency
                }*/

                updated = !(this.index && internals.nextMsn(M3U8Playlist.castAsMedia(this.index)) === internals.nextMsn(M3U8Playlist.castAsMedia(index)));

                this.#index = index;
                this._updatecheck(updated);
            }
            catch (err) {
                if (stream) {
                    stream.destroy();
                }

                if (!this.readable) {
                    return;
                }

                this.emit('error', err);
                this._updatecheck(false);
            }
        };

        const url = this.url;
        if (this.index && this.index.server_control) {
            const index = M3U8Playlist.castAsMedia(this.index);
            assert(index.server_control);
            const canBlock = index.server_control.get('can-block-reload') === 'YES';
            if (canBlock) {
                const partTarget = index.part_info && index.part_info.get('part-target', AttrList.Types.Float);

                if (this.lowLatency && partTarget) {
                    const lastSegment = index.segments.length ? index.segments[index.segments.length - 1] : { uri: undefined, parts: undefined };
                    const hasPartialSegment = !lastSegment.uri;
                    const parts = lastSegment.parts || [];

                    // TODO: detect when playlist is behind server, and guess future part instead / CDN tunein

                    // Params must appear in UTF-8 order

                    url.searchParams.set('_HLS_msn', `${index.lastSeqNo() + +!hasPartialSegment}`);
                    url.searchParams.set('_HLS_part', `${hasPartialSegment ? parts.length : 0}`);
                }
                else {
                    url.searchParams.set('_HLS_msn', `${internals.nextMsn(index)}`);
                }
            }
        }

        perform(Helpers.fetch(url.href, { timeout: 30 * 1000 }));
    }

    _updatecheck(updated = false) {

        try {
            if (updated) {
                assert(this.index);
                this.emit('index', new M3U8Playlist(this.index));

                if (this.index.master) {
                    return this.push(null);          // We are done
                }

                const index = M3U8Playlist.castAsMedia(this.index);

                if (this.#readState.next.isEmpty()) {
                    this.#readState.next = this._initialSegmentPointer(index);
                }
                else if (this.#readState.next.msn < index.startSeqNo(true)) {
                    // playlist skipped ahead for whatever reason
                    this.#readState.discont = true;
                    this.#readState.next = new SegmentPointer(index.startSeqNo(true), this.lowLatency ? 0 : undefined);
                }

                // Check active streams to stop receiving expired segments

                for (const [msn, object] of this.#active) {
                    if (!index.isValidSeqNo(msn)) {
                        this.#active.delete(msn);

                        setTimeout(object.stream.destroy.bind(object.stream), (index.target_duration || 0) * 1000, object.stream);
                    }
                }
            }

            this._checkNext();

            if (this.index && !this.index.ended && this.readable) {
                const index = M3U8Playlist.castAsMedia(this.index);
                this._stallCheck();

                const canBlock = index.server_control ? index.server_control.get('can-block-reload') === 'YES' : false;
                const delaySecs = canBlock ? 0 : Math.max(0.1, this._getUpdateInterval(index, updated));

                this.#updateTimer = setTimeout(this._updateindex.bind(this), delaySecs * 1000);

                // TODO: watch fs for file based urls
            }
        }
        catch (err) {
            this.emit('error', err);
        }
    }
};
