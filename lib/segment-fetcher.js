/* eslint-env node, es6 */

'use strict';

const Hoek = require('hoek');
const UriStream = require('uristream');


const internals = {
    defaults: {
        probe: false
    }
};


const SegmentFetcher = module.exports = function SegmentFetcher(options) {

    options = Hoek.applyToDefaults(internals.defaults, options);

    this.probe = ~~options.probe;

    this.streams = [];
};


SegmentFetcher.prototype.fetchUri = function (uri, byterange, callback) {

    const streamOptions = { probe: this.probe, highWaterMark: 100 * 1000 * 1000 };
    if (byterange) {
        streamOptions.start = byterange.offset;
        streamOptions.end = byterange.offset + byterange.length - 1;
    }
    if (this.probe) {
        streamOptions.timeout = 30 * 1000;
    }

    const stream = UriStream(uri, streamOptions);

    const doFinish = (err, meta) => {

        stream.removeListener('meta', onMeta);
        stream.removeListener('end', onFail);
        stream.removeListener('error', onFail);

        if (this.probe) {
            stream.abort();
            return callback(err, meta);
        }

        return callback(err, meta, stream);
    };

    const onMeta = (meta) => {

        return doFinish(null, meta);
    };

    const onFail = (err) => {

        if (!err) {     // defensive programming
            err = new Error('No metadata');
        }

        return doFinish(err);
    };

    stream.on('meta', onMeta);
    stream.on('end', onFail);
    stream.on('error', onFail);

    return this.startTracking(stream);
};


SegmentFetcher.prototype.abort = function () {

    while (this.streams.length > 0) {
        const stream = this.streams.shift();
        this.stopTracking(stream);
        if (!stream._readableState.ended) {
            stream.abort();
        }
    }
};


internals.onStreamEnd = function () {

    this.fetcher.stopTracking(this);
};


internals.onStreamError = function (/*err*/) {

    if (this.fetcher) {    // fetcher can be missing if a previous error handler calls stopTracking()
        this.fetcher.stopTracking(this);
    }
};


SegmentFetcher.prototype.startTracking = function (stream) {

    Hoek.assert(!stream.fetcher, 'A stream can only be tracked once');
    stream.fetcher = this;

    stream.on('end', internals.onStreamEnd);
    stream.on('error', internals.onStreamError);

    this.streams.push(stream);

    return stream;
};


SegmentFetcher.prototype.stopTracking = function (stream) {

    stream.removeListener('end', internals.onStreamEnd);
    stream.removeListener('error', internals.onStreamError);

    const idx = this.streams.indexOf(stream);
    if (idx !== -1) {
        this.streams.splice(idx, 1);
    }

    stream.fetcher = null;

    return stream;
};
