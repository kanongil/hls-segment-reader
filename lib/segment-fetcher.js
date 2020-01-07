'use strict';

const Hoek = require('@hapi/hoek');
const UriStream = require('uristream');


const internals = {
    defaults: {
        probe: false
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


module.exports = class SegmentFetcher {

    constructor(options) {

        options = Hoek.applyToDefaults(internals.defaults, options);

        this.probe = ~~options.probe;
        this.streams = [];
    }

    fetchUri(uri, byterange, callback) {

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
                stream.destroy();
                return callback(err, meta);
            }

            return callback(err, meta, stream);
        };

        const onMeta = (meta) => {

            if (byterange) {
                meta = Object.assign({}, meta, { size: byterange.length });
            }

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
    }

    abort() {

        while (this.streams.length > 0) {
            const stream = this.streams.shift();
            this.stopTracking(stream);
            if (!stream._readableState.ended) {
                stream.destroy();
            }
        }
    }


    startTracking(stream) {

        Hoek.assert(!stream.fetcher, 'A stream can only be tracked once');
        stream.fetcher = this;

        stream.on('end', internals.onStreamEnd);
        stream.on('error', internals.onStreamError);

        this.streams.push(stream);

        return stream;
    }

    stopTracking(stream) {

        stream.removeListener('end', internals.onStreamEnd);
        stream.removeListener('error', internals.onStreamError);

        const idx = this.streams.indexOf(stream);
        if (idx !== -1) {
            this.streams.splice(idx, 1);
        }

        stream.fetcher = null;

        return stream;
    }
};
