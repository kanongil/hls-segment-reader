'use strict';

const Hoek = require('@hapi/hoek');
const UriStream = require('uristream');


const internals = {
    defaults: {
        probe: false
    }
};


module.exports = class SegmentFetcher {

    static #trackers = new WeakMap();

    #streams = [];

    constructor(options) {

        options = Hoek.applyToDefaults(internals.defaults, options);

        this.probe = !!options.probe;
    }

    fetchUri(uri, byterange) {

        const streamOptions = { probe: this.probe, highWaterMark: 100 * 1000 * 1000 };
        if (byterange) {
            streamOptions.start = byterange.offset;
            streamOptions.end = byterange.offset + byterange.length - 1;
        }

        if (this.probe) {
            streamOptions.timeout = 30 * 1000;
        }

        const stream = UriStream(uri, streamOptions);

        this.startTracking(stream);

        return new Promise((resolve, reject) => {

            const doFinish = (err, meta) => {

                stream.removeListener('meta', onMeta);
                stream.removeListener('end', onFail);
                stream.removeListener('error', onFail);

                if (this.probe) {
                    stream.destroy();
                    return err ? reject(err) : resolve({ meta });
                }

                return err ? reject(err) : resolve({ meta, stream });
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
        });
    }

    abort() {

        while (this.#streams.length > 0) {
            const stream = this.#streams.shift();
            this.stopTracking(stream);
            if (!stream._readableState.ended) {
                stream.destroy();
            }
        }
    }

    startTracking(stream) {

        Hoek.assert(!SegmentFetcher.#trackers.has(stream), 'A stream can only be tracked once');

        stream.on('close', SegmentFetcher.onStreamClose);

        this.#streams.push(stream);
        SegmentFetcher.#trackers.set(stream, this);
    }

    stopTracking(stream) {

        stream.removeListener('close', SegmentFetcher.onStreamClose);

        const idx = this.#streams.indexOf(stream);
        if (idx !== -1) {
            this.#streams.splice(idx, 1);
        }
    }

    static onStreamClose() {

        SegmentFetcher.#trackers.get(this).stopTracking(this);
    }
};
