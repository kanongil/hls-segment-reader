'use strict';

const Url = require('url');

const M3U8Parse = require('m3u8parse');
const Oncemore = require('oncemore');
const UriStream = require('uristream');
const Readable = require('readable-stream');

const SegmentFetcher = require('./segment-fetcher');
const HlsSegmentObject = require('./segment-object');

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
    ])
};


internals.isSameMap = function (m1, m2) {

    return m1 && m2 && m1.uri === m2.uri && m1.byterange === m2.byterange;
};


exports = module.exports = class HlsSegmentReader extends Readable {

    index = null;

    #readState = {
        nextSeq: -1,
        mapSeq: -1,
        active: false,
        fetching: null,
        discont: false
    };
    #watch = new Map(); // used to stop buffering on expired segments
    #fetcher;
    #indexStallSince = null;
    #updateTimer;

    constructor(src, options) {

        options = options || {};
        if (typeof src === 'string') {
            src = Url.parse(src);
        }

        super({ objectMode: true, highWaterMark: options.highWaterMark || 0 });

        this.url = src;
        this.baseUrl = src;

        this.fullStream = !!options.fullStream;
        this.withData = !!options.withData;

        // dates are inclusive
        this.startDate = options.startDate ? new Date(options.startDate) : null;
        this.stopDate = options.stopDate ? new Date(options.stopDate) : null;

        this.maxStallTime = options.maxStallTime || Infinity;

        this.extensions = options.extensions || {};

        this.#fetcher = new SegmentFetcher({ probe: !this.withData });

        this._updateindex();
    }

    abort(graceful) {

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

    _destroy(err, cb) {

        this.abort(!!err);

        return super._destroy(err, cb);
    }

    get indexMimeTypes() {

        return internals.indexMimeTypes;
    }

    get segmentMimeTypes() {

        return internals.segmentMimeTypes;
    }

    _read(/*n*/) {

        this.#readState.active = true;
        this._checkNext();
    }

    // Private methods

    _checkNext() {

        const state = this.#readState;
        const index = this.index;
        if (!this.readable || !state.active || state.fetching || state.nextSeq === -1 || !index) {
            return null;
        }

        let seq = state.nextSeq;
        const segment = index.getSegment(seq, true);

        if (segment) {
            // mark manual discontinuities
            const stateDiscont = state.discont;
            if (stateDiscont) {
                segment.discontinuity = true;
                state.discont = false;
                state.map = null;
            }

            // Check if we need to stop

            if (this.stopDate && segment.program_time > this.stopDate) {
                return this.push(null);
            }

            if (segment.map) {
                if (!stateDiscont && internals.isSameMap(segment.map, state.map)) {
                    delete segment.map;
                }
                else {
                    seq = --state.mapSeq; // signal to fetch init segment
                }
            }

            state.fetching = this._fetchFrom(seq, segment);
            state.fetching.finally(() => {

                state.fetching = null;
            }).then((object) => {

                if (!this.readable) {
                    return;
                }

                if (seq === state.nextSeq) {
                    state.nextSeq++;
                }

                if (object.file.modified) {
                    const segmentTime = segment.program_time || new Date(+object.file.modified - segment.duration * 1000);
                    if (this.startDate && segmentTime < this.startDate) {
                        // too early - drop segment
                        if (object.stream) {
                            object.stream.destroy();
                        }

                        return internals.checkNext(this);
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
                    this.#watch.set(seq, object.stream);
                    Oncemore(object.stream).once('end', 'error', () => {

                        this.#watch.delete(seq);
                    });
                }

                if (seq < 0) {
                    state.map = segment.map;
                }

                state.active = this.push(object);

                this._checkNext();
            }).catch(this.emit.bind(this, 'error'));
        }
        else if (index.ended) {
            this.push(null);
        }
        else if (!index.type && (index.lastSeqNo() < state.nextSeq - 1)) {
            // handle live stream restart
            state.discont = true;
            state.nextSeq = index.startSeqNo(true);
            this._checkNext();
        }
    }

    async _fetchFrom(seqNo, segment) {

        let uri = segment.uri;
        let byterange = segment.byterange;

        if (seqNo < 0) {

            // Fetch init segment

            uri = segment.map.quotedString('uri');
            if (segment.map.byterange) {
                const values = segment.map.quotedString('byterange').split('@');
                byterange = {
                    offset: values.length > 1 ? parseInt(values[1], 10) : 0,
                    length: parseInt(values[0], 10)
                };
            }
            else {
                byterange = null;
            }

            segment = segment.map;
        }

        const { meta, stream } = await this.#fetcher.fetchUri(Url.resolve(this.baseUrl, uri), byterange);

        // Validate returned mime-type

        if (!this.segmentMimeTypes.has(meta.mime.toLowerCase())) {
            if (stream) {
                stream.destroy();
            }

            throw new Error(`Unsupported segment MIME type: ${meta.mime}`);
        }

        return new HlsSegmentObject(meta, stream, seqNo, segment);
    }

    _getUpdateInterval(updated) {

        if (updated && this.index.segments.length) {
            this.#indexStallSince = null;
            return Math.min(this.index.target_duration, this.index.segments[this.index.segments.length - 1].duration);
        }

        if (this.#indexStallSince !== null) {
            if ((Date.now() - this.#indexStallSince) > this.maxStallTime) {
                return -1;
            }
        }
        else {
            this.#indexStallSince = Date.now();
        }

        return this.index.target_duration / 2;
    }

    _initialSeqNo() {

        if (!this.fullStream && this.startDate) {
            const seqNo = this.index.seqNoForDate(this.startDate, true);
            if (seqNo >= 0) {
                return seqNo;
            }

            // no date information in index - it will be approximated from segment metadata
        }

        return this.index.startSeqNo(this.fullStream);
    }

    _updateindex() {

        const stream = UriStream(Url.format(this.url), { timeout: 30 * 1000 });
        stream.on('meta', (meta) => {

            // Check for valid mime type

            if (!this.indexMimeTypes.has(meta.mime.toLowerCase()) &&
                meta.url.indexOf('.m3u8', meta.url.length - 5) === -1 &&
                meta.url.indexOf('.m3u', meta.url.length - 4) === -1) {

                // FIXME: correctly handle .m3u us-ascii encoding
                stream.destroy();

                return stream.emit('error', new Error('Invalid MIME type: ' + meta.mime));
            }

            this.baseUrl = meta.url;
        });

        M3U8Parse(stream, { extensions: this.extensions }).then((index) => {

            if (!this.readable) {
                return;
            }

            let updated = true;
            if (this.index && this.index.lastSeqNo() === index.lastSeqNo()) {
                updated = false;
            }

            this.index = index;
            this._updatecheck(updated);
        }, (err) => {

            if (!this.readable) {
                return;
            }

            this.emit('error', err);
            this._updatecheck(false);
        });
    }

    _updatecheck(updated) {

        if (updated) {
            if (this.#readState.nextSeq === -1) {
                this.#readState.nextSeq = this._initialSeqNo();
            }
            else if (this.#readState.nextSeq < this.index.startSeqNo(true)) {
                // playlist skipped ahead for whatever reason
                this.#readState.discont = true;
                this.#readState.nextSeq = this.index.startSeqNo(true);
            }

            const abortStream = (stream) => {

                if (!stream._readableState.ended) {
                    stream.destroy();
                }
            };

            // check watched segments
            for (const [seq, stream] of this.#watch) {
                if (!this.index.isValidSeqNo(seq)) {
                    this.#watch.delete(seq);

                    setTimeout(abortStream, this.index.target_duration * 1000, stream);
                }
            }

            this.emit('index', this.index);

            if (this.index.master) {
                return this.push(null);
            }
        }

        this._checkNext();

        if (this.index && !this.index.ended && this.readable) {
            const updateInterval = this._getUpdateInterval(updated);
            if (updateInterval <= 0) {
                return this.emit('error', new Error('Index update stalled'));
            }

            this.#updateTimer = setTimeout(this._updateindex.bind(this), Math.max(1, updateInterval) * 1000);
        }
    }
};
