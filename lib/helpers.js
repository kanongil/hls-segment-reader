'use strict';

const Fs = require('fs');
const Url = require('url');

/**
 * @type {(uri: Url.URL | string, options: {}) => NodeJS.ReadStream}
 */
// @ts-ignore
const UriStream = require('uristream');

/**
 * @typedef Uristream.Meta
 * @type {object}
 * @property {string} url
 * @property {string} mime
 * @property {number} size
 * @property {?number} modified
 * @property {string} [etag]
 */

/**
 * @typedef Byterange
 * @type {object}
 * @property {number} offset
 * @property {number} length
*/

/**
 * @typedef FetchResult
 * @type {object}
 * @property {Uristream.Meta} meta
 * @property {ReturnType<UriStream>} [stream]
 */


const internals = {
    fetchBuffer: 10 * 1000 * 1000,
    nothing: Symbol('nothing')
};

/**
 * @template T
 */
exports.deferred = class {

    constructor() {

        /** @type {Promise<T>} */
        this.promise = new Promise((resolve, reject) => {

            /** @type {(arg: T) => void} */
            this.resolve = resolve;

            /** @type {(err: Error) => void} */
            this.reject = reject;
        });
    }
};


/**
 * @param {Url.URL | string} uri
 * @param {{ byterange?: Byterange, probe?: boolean, timeout?: number}} options
 *
 * @return {Promise<FetchResult> & { stream: NodeJS.ReadStream, abort: () => void }}
 */
exports.fetch = function (uri, { byterange, probe = false, timeout } = {}) {

    const streamOptions = Object.assign({
        probe,
        highWaterMark: internals.fetchBuffer,
        timeout: probe ? 30 * 1000 : timeout
    }, byterange ? {
        start: byterange.offset,
        end: byterange.offset + byterange.length - 1
    } : undefined);

    const stream = UriStream(uri, streamOptions);

    /**
     * @type ReturnType<exports.fetch>
     */
    // @ts-ignore
    const promise = new Promise((resolve, reject) => {

        /**
         * @param {?Error} [err]
         * @param {Uristream.Meta} [meta]
         */
        const doFinish = (err, meta) => {

            stream.removeListener('meta', onMeta);
            stream.removeListener('end', onFail);
            stream.removeListener('error', onFail);

            if (err || !meta) {
                return reject(err);
            }

            const result = { meta, stream };
            if (probe) {
                stream.destroy();
                delete result.stream;
            }

            return err ? reject(err) : resolve(result);
        };

        /**
         * @param {Uristream.Meta} meta
         */
        const onMeta = (meta) => {

            meta = Object.assign({}, meta, byterange ? { size: byterange.length } : undefined);

            return doFinish(null, meta);
        };

        /**
         * @param {Error} [err]
         */
        const onFail = (err) => {

            if (!err) {
                err = new Error('No metadata');
            }

            return doFinish(err);
        };

        stream.on('meta', onMeta);
        stream.on('end', onFail);
        stream.on('error', onFail);
    });

    promise.stream = stream;
    promise.abort = () => stream.destroy(new Error('Aborted'));

    return promise;
};

/*
exports.FsWatcher = class {

    #last = internals.nothing;
    #error;
    #deferred;

    constructor(uri) {

        const change = (arg) => {

            if (this.#deferred) {
                this.#deferred.resolve(arg);
                this.#deferred = null;
            }

            this.#last = arg;
        };

        const error = (err) => {

            if (this.#deferred) {
                this.#deferred.reject(err);
                this.#deferred = null;
            }

            this.#error = err;
        };

        const watcher = this.watcher = Fs.watch(Url.fileURLToPath(uri), { persistent: false });

        watcher.on('change', change);
        watcher.on('error', error);
        watcher.once('close', () => {

            watcher.removeListener('change', change);
            watcher.removeListener('error', error);
            this.#last = internals.nothing;
        });
    }

    // Returns latest event since last call, or waits for next

    next() {

        if (this.#error) {
            throw this.#error;
        }

        const last = this.#last;
        if (last !== internals.nothing) {
            this.#last = internals.nothing;
            return last;
        }

        this.#deferred = {};
        this.#deferred.promise = new Promise((resolve, reject) => {

            this.#deferred.resolve = resolve;
            this.#deferred.reject = reject;
        });

        return this.#deferred.promise;
    }

    close() {

        this.watcher.close();

        if (this.#deferred) {
            this.#deferred.reject(new Error('closed'));
            this.#deferred = null;
        }
    }
};*/
