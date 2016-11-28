/* eslint-env node, es6 */

'use strict';

const Url = require('url');
const Util = require('util');

const M3U8Parse = require('m3u8parse');
const Oncemore = require('oncemore');
const UriStream = require('uristream');

const Readable = require('readable-stream');


const internals = {};


internals.fetchFrom = function (reader, seqNo, segment, callback) {

    const segmentUrl = Url.resolve(reader.baseUrl, segment.uri);
    const probe = !reader.withData;

    const streamOptions = { probe: probe, highWaterMark: 100 * 1000 * 1000 };
    if (segment.byterange) {
        streamOptions.start = segment.byterange.offset;
        streamOptions.end = segment.byterange.offset + segment.byterange.length - 1;
    }
    if (probe) {
        streamOptions.timeout = 30 * 1000;
    }

    const stream = UriStream(segmentUrl, streamOptions);

    const finish = (err, res) => {

        stream.removeListener('meta', onmeta);
        stream.removeListener('end', onfail);
        stream.removeListener('error', onfail);
        callback(err, res);
    };

    const onmeta = (meta) => {

        if (!reader.segmentMimeTypes[meta.mime.toLowerCase()]) {
            stream.abort();
            return stream.emit('error', new Error('Unsupported segment MIME type: ' + meta.mime));
        }

        finish(null, new exports.HlsSegmentObject(seqNo, segment, meta, probe ? null : stream));
    };

    const onfail = (err) => {

        if (!err) {     // defensive programming
            err = new Error('No metadata');
        }

        finish(err);
    };

    stream.on('meta', onmeta);
    stream.on('end', onfail);
    stream.on('error', onfail);

    return stream;
};


internals.checkNext = function (reader) {

    const state = reader.readState;
    const index = reader.index;
    if (!reader.readable || !state.active || state.fetching || state.nextSeq === -1 || !index) {
        return null;
    }

    const seq = state.nextSeq;
    const segment = index.getSegment(seq, true);

    if (segment) {
        // mark manual discontinuities
        if (state.discont) {
            segment.discontinuity = true;
            state.discont = false;
        }

        // Check if we need to stop

        if (reader.stopDate && segment.program_time > reader.stopDate) {
            return reader.push(null);
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
                if (object.stream) {
                    reader.watch[object.seq] = object.stream;
                    Oncemore(object.stream).once('end', 'error', () => {

                        delete reader.watch[object.seq];
                    });
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


const HlsSegmentReader = function (src, options) {

    if (!(this instanceof HlsSegmentReader)) {
        return new HlsSegmentReader(src, options);
    }

    options = options || {};
    if (typeof src === 'string') {
        src = Url.parse(src);
    }

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
        active: false,
        fetching: null,
        discont: false
    };
    this.watch = {}; // used to stop buffering on expired segments

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
            return index.seqNoForDate(this.startDate, true);
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

            if (!this.indexMimeTypes[meta.mime.toLowerCase()] &&
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

    Readable.call(this, { objectMode: true, highWaterMark: options.highWaterMark || 0 });

    updateindex();
};
Util.inherits(HlsSegmentReader, Readable);


HlsSegmentReader.prototype.abort = function (graceful) {

    if (!graceful) {

        // Abort all active streams

        const state = this.readState;
        if (state.fetching && !state.fetching._readableState.ended) {
            state.fetching.abort();
        }

        for (const seq in this.watch) {
            const stream = this.watch[seq];
            delete this.watch[seq];
            if (!stream._readableState.ended) {
                stream.abort();
            }
        }
    }

    if (!this.readable) {
        return;
    }

    if (!this._readableState.ended) {
        this.push(null);
    }

    this.readable = false;
};


HlsSegmentReader.prototype.destroy = function () {

    return this.abort();
};


HlsSegmentReader.prototype.indexMimeTypes = {
    'application/vnd.apple.mpegurl': true,
    'application/x-mpegurl': true,
    'audio/mpegurl': true
};


HlsSegmentReader.prototype.segmentMimeTypes = {
    'video/mp2t': true,
    'video/mpeg': true,
    'video/mp4': true,
    'audio/aac': true,
    'audio/x-aac': true,
    'audio/ac3': true,
    'audio/vnd.dolby.dd-raw': true,
    'audio/x-ac3': true,
    'audio/eac3': true,
    'text/vtt': true
};


HlsSegmentReader.prototype._read = function (/*n*/) {

    this.readState.active = true;
    internals.checkNext(this);
};


exports = module.exports = HlsSegmentReader;

exports.HlsSegmentObject = function (seq, segment, fileMeta, stream) {

    this.seq = seq;
    this.details = segment;
    this.file = fileMeta;
    this.stream = stream;
};
