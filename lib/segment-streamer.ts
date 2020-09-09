
import type { HlsSegmentReader } from './segment-reader';
import type { M3U8Segment, MediaPlaylist, M3U8IndependentSegment } from 'm3u8parse/lib/m3u8playlist';
import type { ReadableStream, FetchResult } from './helpers';

import { Stream, finished } from 'stream';

import { assert as hoekAssert } from '@hapi/hoek';
import { AttrList } from 'm3u8parse/lib/attrlist';
import { M3U8Playlist } from 'm3u8parse/lib/m3u8playlist';
import { Transform } from 'readable-stream';

import { SegmentDownloader } from './segment-downloader';
import { HlsSegmentObject } from './segment-object';

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


// eslint-disable-next-line func-style
function assert(condition: any, ...args: any[]): asserts condition {

    hoekAssert(condition, ...args);
}


const internals = {
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

    isSameMap(m1: AttrList, m2: AttrList) {

        return m1 && m2 && m1.get('uri') === m2.get('uri') && m1.get('byterange') === m2.get('byterange');
    }
};

export type HlsSegmentStreamerOptions = {
    highWaterMark?: number;
    withData?: boolean; // default true
    lowLatency?: boolean; // default true
}

export class HlsSegmentStreamer extends Transform {

    withData: boolean;
    lowLatency: boolean;

    #readState = {
        /** @type {Set<number>} */
        indexTokens: new Set(),
        /** @type {Set<number>} */
        activeTokens: new Set(),

        mapSeq: -1,
        /** @type {AttrList | undefined} */
        map: undefined,
        /** @type {?Promise<HlsSegmentObject>} */
        fetching: null,
        active: false,
        discont: false
    };

    #active = new Map<number, HlsSegmentObject & { stream?: ReadableStream & { addParts?: Function, addHint?: Function }}>(); // used to stop buffering on expired segments
    #downloader: SegmentDownloader;
    #reader?: HlsSegmentReader;

    constructor(reader?: HlsSegmentReader, options: HlsSegmentStreamerOptions = {}) {

        super({ objectMode: true, highWaterMark: (reader || {} as any).highWaterMark || options.highWaterMark || 0 });

        if (typeof reader === 'object' && !(reader instanceof Stream)) {
            options = /** @type {HlsSegmentStreamerOptions} */(reader);
            reader = undefined;
        }

        this.withData = options.withData ?? true;
        this.lowLatency = options.lowLatency ?? true;

        this.#downloader = new SegmentDownloader({ probe: !this.withData });

        this.on('pipe', (src: HlsSegmentReader) => {

            assert(!this.#reader, 'Only one piped source is supported');

            this.#reader = src;
            src.on('index', (index: M3U8Playlist) => {

                this.emit('index', index);
            });
        });

        this.on('unpipe', () => {

            this.#reader = undefined;
        });

        // Pipe to self

        if (reader) {
            reader.on('error', (err) => {

                if (!this.destroyed) {
                    this.destroy(err);
                }
            }).pipe(this);
        }
    }

    abort(graceful = false) {

        if (!graceful) {
            this.#downloader.setValid();
        }

        if (!this.readable) {
            return;
        }

        if (!this.readableEnded) {
            this.push(null);
        }
    }

    _destroy(err: Error | null, cb: any) {

        // FIXME: is reader unpiped first???
        if (this.#reader && !this.#reader.destroyed) {
            this.#reader.destroy(err || undefined);
        }

        super._destroy(err, cb);

        this.abort(!!err);
    }

    get index() {

        return this.#reader ? this.#reader.index : undefined;
    }

    get segmentMimeTypes() {

        return internals.segmentMimeTypes;
    }

    validateSegmentMeta(meta: FetchResult['meta']) {

        // Check for valid mime type

        if (!this.segmentMimeTypes.has(meta.mime.toLowerCase())) {
            throw new Error(`Unsupported segment MIME type: ${meta.mime}`);
        }
    }

    _transform(segment: { msn: number, entry: M3U8IndependentSegment }, _: unknown, done: (err?: Error) => void) {

        assert(typeof segment.msn === 'number' && segment.entry, 'Only segment-reader segments are supported');

        this._process(segment).then(done.bind(null, undefined), done);
    }

    // Private methods

    async _process(segment: { msn: number, entry: M3U8IndependentSegment }): Promise<HlsSegmentObject | null> {

        // Update active token list

        if (this.#reader && this.#reader.index && !this.#reader.index.master) {
            this._updateTokens(M3U8Playlist.castAsMedia(this.#reader.index));
            this.#downloader.setValid(this.#readState.activeTokens);
        }

        // Fetch the segment

        const object = await this._fetchFrom({ msn: segment.msn }, segment.entry);

        // At this point object.stream has only been readied / opened

        const drop = function (err?: Error) {

            if (object.stream) {
                object.stream.destroy(err);
            }

            if (err) {
                throw err;
            }

            return null;
        };

        // Check meta

        if (this.#reader && object.file.modified) {
            const segmentTime = segment.entry.program_time || new Date(+object.file.modified - (segment.entry.duration || 0) * 1000);

            if (this.#reader.startDate && segmentTime < this.#reader.startDate) {
                return drop(new Error('too early'));
            }

            if (this.#reader.stopDate && segmentTime > this.#reader.stopDate) {
                return drop();
            }
        }

        // Track embedded stream to append more parts later

        if (object.stream) {
            this.#active.set(segment.msn, object);
            finished(object.stream, () => this.#active.delete(segment.msn));
        }

        return object;
    }

    /**
     * @param {SegmentPointer} ptr
     * @param {M3U8IndependentSegment} segment
     */
    async _fetchFrom(ptr, segment: M3U8IndependentSegment) {

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
                    uri: part.get('uri', AttrList.Types.String),
                    /** @type {Required<Helpers.Byterange> | undefined} */
                    byterange: part.has('byterange') ? Object.assign({ offset: 0 }, part.get('byterange', AttrList.Types.Byterange)) : undefined
                }));
                fetch = await this.#downloader.fetchParts(this._tokenForMsn(ptr.msn), parts, !segment.isPartial());
            }
            else {
                fetch = await this.#downloader.fetchSegment(this._tokenForMsn(ptr.msn), uri, byterange);
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

    _tokenForMsn(msn: number) {

        return msn; // TODO: handle start over
    }

    _updateTokens(index: MediaPlaylist) {

        const old = this.#readState.indexTokens;

        const current = new Set<number>();
        for (let i = index.startSeqNo(true); i < index.lastSeqNo(true); ++i) {
            const token = this._tokenForMsn(i);
            current.add(token);
            old.delete(token);
        }

        this.indexTokens = current;
        if (this.#readState.discont) {
            this.#readState.activeTokens = this.indexTokens;
        }
        else {
            // Keep expired tokens until next update

            this.#readState.activeTokens = new Set([...old, ...current]);
        }
    }




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
                        uri: part.get('uri', AttrList.Types.String),
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
};
