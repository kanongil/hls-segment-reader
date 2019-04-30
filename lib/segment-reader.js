'use strict';

const Url = require('url');

const M3U8Parse = require('m3u8parse');
const Oncemore = require('oncemore');
const UriStream = require('uristream');
const Readable = require('readable-stream');

const SegmentFetcher = require('./segment-fetcher');
const HlsSegmentObject = require('./segment-object');


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


internals.fetchFrom = function (reader, seqNo, segment, callback) {

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

    return reader.fetcher.fetchUri(Url.resolve(reader.baseUrl, uri), byterange, (err, meta, stream) => {

        if (err) {
            return callback(err);
        }

        if (!reader.segmentMimeTypes.has(meta.mime.toLowerCase())) {
            if (stream) {
                stream.abort();
            }

            return callback(new Error(`Unsupported segment MIME type: ${meta.mime}`));
        }

        return callback(null, new HlsSegmentObject(meta, stream, seqNo, segment));
    });
};


internals.isSameMap = function (m1, m2) {

    return m1 && m2 && m1.uri === m2.uri && m1.byterange === m2.byterange;
};


internals.checkNext = function (reader) {

    const state = reader.readState;
    const index = reader.index;
    if (!reader.readable || !state.active || state.fetching || state.nextSeq === -1 || !index) {
        return null;
    }

    let seq = state.nextSeq;
    const segment = index.getSegment(seq, true);

    if (segment) {
        // mark manual discontinuities
        if (state.discont) {
            segment.discontinuity = true;
            state.discont = false;
            state.map = null;
        }

        // Check if we need to stop

        if (reader.stopDate && segment.program_time > reader.stopDate) {
            return reader.push(null);
        }

        if (segment.map) {
            if (internals.isSameMap(segment.map, state.map)) {
                delete segment.map;
            }
            else {
                seq = --state.mapSeq; // signal to fetch init segment
            }
        }

        state.fetching = internals.fetchFrom(reader, seq, segment, (err, object) => {

            if (!reader.readable) {
                return;
            }

            state.fetching = null;
            if (err) {
                reader.emit('error', err);
            }

            if (seq === state.nextSeq) {
                state.nextSeq++;
            }

            if (object) {
                if (object.file.modified) {
                    const segmentTime = segment.program_time || new Date(+object.file.modified - segment.duration * 1000);
                    if (reader.startDate && segmentTime < reader.startDate) {
                        // too early - drop segment
                        if (object.stream) {
                            object.stream.abort();
                        }

                        return internals.checkNext(reader);
                    }

                    if (reader.stopDate && segmentTime > reader.stopDate) {
                        // check that this is also valid for next segment with date
                        if (object.stream) {
                            object.stream.abort();
                        }

                        return reader.push(null);
                    }
                }

                if (object.stream) {
                    reader.watch[seq] = object.stream;
                    Oncemore(object.stream).once('end', 'error', () => {

                        delete reader.watch[seq];
                    });
                }

                if (seq < 0) {
                    state.map = segment.map;
                }

                state.active = reader.push(object);
            }

            internals.checkNext(reader);
        });
    }
    else if (index.ended) {
        reader.push(null);
    }
    else if (!index.type && (index.lastSeqNo() < state.nextSeq - 1)) {
        // handle live stream restart
        state.discont = true;
        state.nextSeq = index.startSeqNo(true);
        internals.checkNext(reader);
    }
};


exports = module.exports = class HlsSegmentReader extends Readable {

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

        this.index = null;
        this.readState = {
            nextSeq: -1,
            mapSeq: -1,
            active: false,
            fetching: null,
            discont: false
        };
        this.watch = {}; // used to stop buffering on expired segments
        this.fetcher = new SegmentFetcher({ probe: !this.withData });

        this.indexStallSince = null;

        const getUpdateInterval = (updated) => {

            if (updated && this.index.segments.length) {
                this.indexStallSince = null;
                return Math.min(this.index.target_duration, this.index.segments[this.index.segments.length - 1].duration);
            }

            if (this.indexStallSince !== null) {
                if ((Date.now() - this.indexStallSince) > this.maxStallTime) {
                    return -1;
                }
            }
            else {
                this.indexStallSince = Date.now();
            }

            return this.index.target_duration / 2;
        };

        const initialSeqNo = () => {

            const index = this.index;

            if (!this.fullStream && this.startDate) {
                const seqNo = index.seqNoForDate(this.startDate, true);
                if (seqNo >= 0) {
                    return seqNo;
                }

                // no date information in index - it will be approximated from segment metadata
            }

            return index.startSeqNo(this.fullStream);
        };

        const updatecheck = (updated) => {

            if (updated) {
                if (this.readState.nextSeq === -1) {
                    this.readState.nextSeq = initialSeqNo();
                }
                else if (this.readState.nextSeq < this.index.startSeqNo(true)) {
                    // playlist skipped ahead for whatever reason
                    this.readState.discont = true;
                    this.readState.nextSeq = this.index.startSeqNo(true);
                }

                const abortStream = (stream) => {

                    if (!stream._readableState.ended) {
                        stream.abort();
                    }
                };

                // check watched segments
                for (const seq in this.watch) {
                    if (!this.index.isValidSeqNo(seq)) {
                        const stream = this.watch[seq];
                        delete this.watch[seq];

                        setTimeout(abortStream, this.index.target_duration * 1000, stream);
                    }
                }

                this.emit('index', this.index);

                if (this.index.master) {
                    return this.push(null);
                }
            }

            internals.checkNext(this);

            if (this.index && !this.index.ended && this.readable) {
                const updateInterval = getUpdateInterval(updated);
                if (updateInterval <= 0) {
                    return this.emit('error', new Error('Index update stalled'));
                }

                setTimeout(updateindex, Math.max(1, updateInterval) * 1000);
            }
        };

        const updateindex = () => {

            if (!this.readable) {
                return;
            }

            const stream = UriStream(Url.format(this.url), { timeout: 30 * 1000 });
            stream.on('meta', (meta) => {

                // Check for valid mime type

                if (!this.indexMimeTypes.has(meta.mime.toLowerCase()) &&
                        meta.url.indexOf('.m3u8', meta.url.length - 5) === -1 &&
                        meta.url.indexOf('.m3u', meta.url.length - 4) === -1) {

                    // FIXME: correctly handle .m3u us-ascii encoding
                    stream.abort();

                    return stream.emit('error', new Error('Invalid MIME type: ' + meta.mime));
                }

                this.baseUrl = meta.url;
            });

            M3U8Parse(stream, { extensions: this.extensions }, (err, index) => {

                if (!this.readable) {
                    return;
                }

                if (err) {
                    this.emit('error', err);
                    updatecheck(false);
                }
                else {
                    let updated = true;
                    if (this.index && this.index.lastSeqNo() === index.lastSeqNo()) {
                        updated = false;
                    }

                    this.index = index;
                    updatecheck(updated);
                }
            });
        };

        updateindex();
    }

    abort(graceful) {

        if (!graceful) {
            this.fetcher.abort();
        }

        if (!this.readable) {
            return;
        }

        if (!this._readableState.ended) {
            this.push(null);
        }

        this.readable = false;
    }

    destroy() {

        return this.abort();
    }

    get indexMimeTypes() {

        return internals.indexMimeTypes;
    }

    get segmentMimeTypes() {

        return internals.segmentMimeTypes;
    }

    _read(/*n*/) {

        this.readState.active = true;
        internals.checkNext(this);
    }
};
