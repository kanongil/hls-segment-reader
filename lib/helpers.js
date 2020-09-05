'use strict';

/** @typedef { import('m3u8parse/lib/m3u8playlist').MediaPlaylist } MediaPlaylist */

const Fs = require('fs');
const Url = require('url');

/**
 * @typedef ReadableStream
 * @type {NodeJS.ReadableStream & { destroy(err?: Error) : void, destroyed: boolean }}
 */

/**
 * @type {(uri: Url.URL | string, options: {}) => ReadableStream}
 */
// @ts-ignore
const UriStream = require('uristream');
const { AttrList } = require('m3u8parse');

/**
 * @typedef Uristream.Meta
 * @type {object}
 * @property {string} url
 * @property {string} mime
 * @property {number} size
 * @property {?number} modified
 * @property {string | undefined} [etag]
 */

/**
 * @typedef Byterange
 * @type {object}
 * @property {number} offset
 * @property {number | undefined} [length]
*/

/**
 * @typedef FetchResult
 * @type {object}
 * @property {Uristream.Meta} meta
 * @property {ReadableStream | undefined} [stream]
 */


const internals = {
    fetchBuffer: 10 * 1000 * 1000,
    nothing: Symbol('nothing')
};

/**
 * @template T
 */
exports.Deferred = class {

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
 * @return {Promise<FetchResult> & {abort: () => void }}
 */
exports.fetch = function (uri, { byterange, probe = false, timeout } = {}) {

    const streamOptions = Object.assign({
        probe,
        highWaterMark: internals.fetchBuffer,
        timeout: probe ? 30 * 1000 : timeout,
        retries: 1
    }, byterange ? {
        start: byterange.offset,
        end: byterange.length !== undefined ? byterange.offset + byterange.length - 1 : undefined
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

            meta = Object.assign({}, meta, (byterange && byterange.length !== undefined) ? { size: byterange.length } : undefined);

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

    promise.abort = () => !stream.destroyed && stream.destroy(new Error('Aborted'));

    return promise;
};


exports.ParsedPlaylist = class {

    /**
     * @param {MediaPlaylist} index
     */
    constructor(index) {

        this.index = index;
    }

    startMsn(full = false) {

        return this.index.startSeqNo(full);
    }

    lastMsn(includePartial = false) {

        return this.index.lastSeqNo(includePartial);
    }

    /**
     * @param {Date} date
     */
    msnForDate(date) {

        return this.index.seqNoForDate(date, true);
    }

    /**
     * @param {number} msn
     * @param {string} baseUri
     */
    getResolvedSegment(msn, baseUri) {

        const segment = this.index.getSegment(msn, true);
        if (segment) {
            segment.rewriteUris((uri) => {

                return uri !== undefined ? new Url.URL(uri, baseUri).href : uri;
            });
        }

        return segment;
    }

    /**
     * @param {MediaPlaylist} index
     */
    isSameHead(index, includePartial = false) {

        const sameMsn = this.lastMsn(includePartial) === index.lastSeqNo(includePartial);
        if (!sameMsn || !includePartial) {
            return sameMsn;
        }

        // Same + partial check

        return ((this.segments[this.segments.length - 1].parts || []).length ===
                (index.segments[index.segments.length - 1].parts || []).length);
    }

    /**
     * @return {{ msn: number, part?: number }} 
     */
    nextHead(includePartial = false) {

        if (includePartial && this.partTarget) {
            const lastSegment = this.segments.length ? this.segments[this.segments.length - 1] : { uri: undefined, parts: undefined };
            const hasPartialSegment = !lastSegment.uri;
            const parts = lastSegment.parts || [];

            return {
                msn: this.lastMsn(true) + +!hasPartialSegment,
                part: hasPartialSegment ? parts.length : 0
            };
        }

        return { msn: this.lastMsn(false) + 1 };
    }

    get segments() {

        return this.index.segments;
    }

    get partTarget() {

        const info = this.index.part_info;
        return info ? info.get('part-target', AttrList.Types.Float) || undefined : undefined;
    }

    get serverControl() {

        const control = this.index.server_control;
        return {
            canBlockReload: control ? control.get('can-block-reload') === 'YES' : false,
            partHoldBack: control ? control.get('part-hold-back', AttrList.Types.Float) || undefined : undefined
        };
    }

    get preloadHints() {

        /** @typedef {{ uri: string, byterange?: { offset: number, length?: number } }} PartData */
        /** @type {{ part?: PartData, map?: PartData }} */
        const hints = {};

        const list = this.index.meta.preload_hints;
        for (const attrs of list || []) {
            const type = attrs.get('type')?.toLowerCase();
            if (attrs.has('uri') && type === 'part' || type === 'map') {
                hints[type] = {
                    uri: attrs.get('uri', AttrList.Types.String) || '',
                    byterange: attrs.has('byterange-start') ? {
                        offset: attrs.get('byterange-start', AttrList.Types.Int),
                        length: (attrs.has('byterange-length') ? attrs.get('byterange-length', AttrList.Types.Int) : undefined)
                    } : undefined
                };
            }
        }

        return hints;
    }
};


exports.FsWatcher = class {

    /** @type {'rename' | 'change' | undefined} */
    #last;

    /** @type {Error | undefined} */
    #error;

    /** @type {?exports.Deferred<'rename' | 'change'>} */
    #deferred = null;

    /**
     * @param {Url.URL | string} uri
     */
    constructor(uri) {

        /**
         * @param {'rename' | 'change'} eventType
         */
        const change = (eventType) => {

            if (this.#deferred) {
                this.#deferred.resolve(eventType);
                this.#deferred = null;
            }

            this.#last = eventType;
        };

        /**
         * @param {Error} err
         */
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
            this.#last = undefined;
        });
    }

    // Returns latest event since last call, or waits for next

    next() {

        if (this.#error) {
            throw this.#error;
        }

        const last = this.#last;
        if (last !== undefined) {
            this.#last = undefined;
            return last;
        }

        this.#deferred = new exports.Deferred();

        return this.#deferred.promise;
    }

    close() {

        if (!this.#error) {
            this.watcher.close();

            if (this.#deferred) {
                this.#deferred.reject(new Error('closed'));
                this.#deferred = null;
            }
        }
    }
};
