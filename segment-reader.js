/* jshint node:true */

'use strict';

var Url = require('url'),
    Util = require('util');

var M3U8Parse = require('m3u8parse'),
    Oncemore = require('oncemore'),
    UriStream = require('uristream');

var Readable = require('readable-stream');


var internals = {};


internals.fetchFrom = function (reader, seqNo, segment, callback) {

    var segmentUrl = Url.resolve(reader.baseUrl, segment.uri);
    var probe = !reader.withData;

    var streamOptions = { probe: probe, highWaterMark: 100 * 1000 * 1000 };
    if (segment.byterange) {
        streamOptions.start = segment.byterange.offset;
        streamOptions.end = segment.byterange.offset + segment.byterange.length - 1;
    }

    var stream = UriStream(segmentUrl, streamOptions);

    var finish = function (err, res) {

        stream.removeListener('meta', onmeta);
        stream.removeListener('end', onfail);
        stream.removeListener('error', onfail);
        callback(err, res);
    };

    var onmeta = function (meta) {

        if (reader.segmentMimeTypes.indexOf(meta.mime.toLowerCase()) === -1) {
            stream.abort();
            return stream.emit('error', new Error('Unsupported segment MIME type: ' + meta.mime));
        }

        finish(null, new exports.HlsSegmentObject(seqNo, segment, meta, probe ? null : stream));
    };

    var onfail = function (err) {

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


internals.checkNext = function checknext (reader) {

    var state = reader.readState;
    var index = reader.index;
    if (!reader.readable || !state.active || state.fetching || state.nextSeq === -1 || !index) {
        return null;
    }

    var seq = state.nextSeq;
    var segment = index.getSegment(seq, true);

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

        state.fetching = internals.fetchFrom(reader, seq, segment, function (err, object) {

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
                    Oncemore(object.stream).once('end', 'error', function () {

                        delete reader.watch[object.seq];
                    });
                }

                state.active = reader.push(object);
            }

            internals.checkNext(reader);
        });
    } else if (index.ended) {
        reader.push(null);
    } else if (!index.type && (index.lastSeqNo() < state.nextSeq - 1)) {
        // handle live stream restart
        state.discont = true;
        state.nextSeq = index.startSeqNo(true);
        internals.checkNext(reader);
    }
};


var HlsSegmentReader = function (src, options) {

    if (!(this instanceof HlsSegmentReader)) {
        return new HlsSegmentReader(src, options);
    }

    var self = this;

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

    var getUpdateInterval = function (updated) {

        if (updated && self.index.segments.length) {
            self.indexStallSince = null;
            return Math.min(self.index.target_duration, self.index.segments[self.index.segments.length - 1].duration);
        }

        if (self.indexStallSince !== null) {
            if ((Date.now() - self.indexStallSince) > self.maxStallTime) {
                return -1;
            }
        } else {
            self.indexStallSince = Date.now();
        }

        return self.index.target_duration / 2;
    };

    var initialSeqNo = function () {

        var index = self.index;

        if (!self.fullStream && self.startDate) {
            return index.seqNoForDate(self.startDate, true);
        }

        return index.startSeqNo(self.fullStream);
    };

    var updatecheck = function (updated) {

        if (updated) {
            if (self.readState.nextSeq === -1) {
                self.readState.nextSeq = initialSeqNo();
            }
            else if (self.readState.nextSeq < self.index.startSeqNo(true)) {
                // playlist skipped ahead for whatever reason
                self.readState.discont = true;
                self.readState.nextSeq = self.index.startSeqNo(true);
            }

            var abortStream = function (stream) {

                if (!stream._readableState.ended) {
                    stream.abort();
                }
            };

            // check watched segments
            for (var seq in self.watch) {
                if (!self.index.isValidSeqNo(seq)) {
                    var stream = self.watch[seq];
                    delete self.watch[seq];

                    setTimeout(abortStream, self.index.target_duration * 1000, stream);
                }
            }

            self.emit('index', self.index);

            if (self.index.variant) {
                return self.push(null);
            }
        }
        internals.checkNext(self);

        if (self.index && !self.index.ended && self.readable) {
            var updateInterval = getUpdateInterval(updated);
            if (updateInterval <= 0) {
                return self.emit('error', new Error('Index update stalled'));
            }

            setTimeout(updateindex, Math.max(1, updateInterval) * 1000);
        }
    };

    var updateindex = function () {

        if (!self.readable) {
            return;
        }

        var stream = UriStream(Url.format(self.url), { timeout: 30 * 1000 });
        stream.on('meta', function (meta) {

            // Check for valid mime type

            if (self.indexMimeTypes.indexOf(meta.mime.toLowerCase()) === -1 &&
                    meta.url.indexOf('.m3u8', meta.url.length - 5) === -1 &&
                    meta.url.indexOf('.m3u', meta.url.length - 4) === -1) {

                // FIXME: correctly handle .m3u us-ascii encoding
                stream.abort();

                return stream.emit('error', new Error('Invalid MIME type: ' + meta.mime));
            }

            self.baseUrl = meta.url;
        });

        M3U8Parse(stream, { extensions: self.extensions }, function (err, index) {

            if (!self.readable) {
                return;
            }

            if (err) {
                self.emit('error', err);
                updatecheck(false);
            } else {
                var updated = true;
                if (self.index && self.index.lastSeqNo() === index.lastSeqNo()) {
                    updated = false;
                }

                self.index = index;
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

        var state = this.readState;
        if (state.fetching && !state.fetching._readableState.ended) {
            state.fetching.abort();
        }

        for (var seq in this.watch) {
            var stream = this.watch[seq];
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


HlsSegmentReader.prototype.indexMimeTypes = [
    'application/vnd.apple.mpegurl',
    'application/x-mpegurl',
    'audio/mpegurl'
];


HlsSegmentReader.prototype.segmentMimeTypes = [
    'video/mp2t',
    'audio/aac',
    'audio/x-aac',
    'audio/ac3',
    'text/vtt'
];


HlsSegmentReader.prototype._read = function (/*n*/) {

    this.readState.active = true;
    internals.checkNext(this);
};


var exports = module.exports = HlsSegmentReader;

exports.HlsSegmentObject = function (seq, segment, fileMeta, stream) {

    this.seq = seq;
    this.details = segment;
    this.file = fileMeta;
    this.stream = stream;
};
