'use strict';

const Stream = require('stream');
const Util = require('util');

const Hoek = require('@hapi/hoek');
const { PassThrough } = require('readable-stream');

const Helpers = require('./helpers');

const internals = {
    defaults: {
        probe: false
    },
    streamFinished: Util.promisify(Stream.finished)
};

/**
 * @typedef Part
 * @type {object}
 * @property {string} uri
 * @property {Helpers.Byterange | undefined} [byterange]
 * @property {boolean | undefined} [final]
 * @property {Hint | undefined} [hint]
 */

/**
 * @typedef Hint
 * @type {object}
 * @property {Part} part
 * @property {ReturnType<Helpers.fetch>} fetch
 */

/**
 * @typedef ExtendedFetch
 * @type {Promise<Helpers.FetchResult & { part: Part }> & { stream: NodeJS.ReadStream, abort: () => void }}
 */

class PartStream extends PassThrough {

    /** @type { Part[] } */
    #queuedParts = [];

    /** @type { ExtendedFetch[] } */
    #fetches = [];

    /** @type {Helpers.deferred<Helpers.FetchResult['meta']> & { queued: boolean }} */
    #meta;

    /** @type {ReturnType<setImmediate> | undefined} */
    #fetchTimer;

    /** @type {Hint | undefined} */
    #hint;

    /**
     * @param {Part[]} parts
     */
    constructor(parts) {

        super();

        this.#meta = Object.assign(new Helpers.deferred(), { queued: false });

        this.addParts(parts);
    }

    /**
     * @param {Part[]} parts
     */
    addParts(parts, final = false) {

        if (!Array.isArray(parts)) {
            throw new TypeError('Parts must be an array');
        }

        Hoek.assert(this.writable, 'Stream cannot be closed');

        if (this.#fetchTimer) {
            clearImmediate(this.#fetchTimer);
        }

        this.#queuedParts.push(...parts);

        /** @type {(hint?: Hint) => void} */
        const start = (hint) => {

            const fetches = this._mergeParts(this.#queuedParts, { final, hint }).map((part) => {

                const resolveMeta = !this.#meta.queued;
                this.#meta.queued = false;

                const fetch = this._fetchPart(part);

                const promise = Object.assign(fetch.then((fetchResult) => {

                    const result = { ...fetchResult, part };

                    if (resolveMeta) {
                        this.#meta.resolve(fetchResult.meta);
                    }

                    return result;
                }), {
                    stream: fetch.stream,
                    abort: () => fetch.abort()
                });

                return promise;
            });

            this._feedFetches(fetches).catch(this.destroy.bind(this));

            this.#queuedParts = [];
        };

        final ? start(this.#hint) : this.#fetchTimer = setImmediate(start, this.#hint);
        this.#hint = undefined;
    }

    /**
     * @param {Part} [part]
     */
    addHint(part) {

        if (this.#hint) {
            this.#hint.fetch.abort();
            this.#hint = undefined;
        }

        if (part) {
            const fetch = this._fetchPart(part);
            fetch.catch(Hoek.ignore);
            this.#hint = { part, fetch };
        }
    }

    /**
     * @param {?Error} err
     * @param {*} cb
     */
    _destroy(err, cb) {

        const fetches = this.#fetches;
        this.#fetches = [];
        for (const fetch of fetches) {
            fetch.catch(Hoek.ignore);
            fetch.abort();
        }

        this.addHint();
        this.#meta.reject(err || new Error('destroyed'));

        super._destroy(err, cb);
    }

    /**
     * @param {ExtendedFetch[]} fetches
     */
    async _feedFetches(fetches) {

        const active = this.#fetches.length > 0;
        this.#fetches.push(...fetches);

        if (!active) {
            for (const fetch of this.#fetches) {
                const { stream, part } = await fetch;
                await this._feedPart(stream, part);
            }

            this.#fetches = [];
        }
    }

    _meta() {

        return this.#meta.promise;
    }

    /**
     * @param {Part[]} parts
     * @param {{ final: boolean, hint?: Hint }} options
     */
    _mergeParts(parts, { final, hint }) {

        if (hint) {
            for (const part of parts) {
                if (part.uri === hint.part.uri &&
                    !(part.byterange || +part.byterange.offset === +hint.part.byterange.offset)) {

                    part.hint = hint;
                    break;
                }
            }
        }

        // optimization - find common parts + ranges

        const merged = parts.slice(0, 1);
        let last = merged[0];
        for (let i = 1; i < parts.length; ++i) {
            const part = parts[i];
            if (!last.hint && !part.hint &&
                last.uri === part.uri &&
                last.byterange && part.byterange) {

                if (part.byterange.offset === last.byterange.offset + last.byterange.length) {

                    last.byterange.length += part.byterange.length;
                    continue;
                }
            }

            merged.push(part);
            last = part;
        }

        if (final) {
            merged[merged.length - 1].final = true;
        }

        return merged;
    }

    /**
     * @param {Part} part
     */
    _fetchPart(part) {

        return part.hint ? part.hint.fetch : Helpers.fetch(part.uri, { byterange: part.byterange });
    }

    /**
     * @param {NodeJS.ReadStream} stream
     * @param {Part} part
     */
    _feedPart(stream, part) {

        // TODO: only feed part.byterange.length in case it is longer??

        stream.pipe(this, { end: !!part.final });
        return internals.streamFinished(stream);
    }
}


module.exports = class SegmentFetcher {

    // @ts-ignore
    static #trackers = new WeakMap();

    /** @type {Set<NodeJS.ReadStream>} */
    #streams = new Set();

    /**
     * @param {{ probe?: boolean }} options
     */
    constructor(options) {

        options = Hoek.applyToDefaults(internals.defaults, options);

        this.probe = !!options.probe;
    }

    /**
     * @param {string} uri
     * @param {Helpers.Byterange} [byterange]
     */
    fetchSegment(uri, byterange) {

        const promise = Helpers.fetch(uri, { byterange, probe: this.probe });
        this._startTracking(promise.stream);
        return promise;
    }

    /**
     * @param {Part[]} parts
     */
    async fetchParts(parts, final = false) {

        const stream = new PartStream(parts);
        if (final) {
            stream.addParts([], true);
        }

        const meta = Object.assign({}, await stream._meta(), { size: -1 });

        return { meta, stream };
    }

    abort() {

        for (const stream of this.#streams) {
            this._stopTracking(stream);
            stream.destroy();
        }
    }

    /**
     * @param {NodeJS.ReadStream} stream
     */
    _startTracking(stream) {

        Hoek.assert(!SegmentFetcher.#trackers.has(stream), 'A stream can only be tracked once');

        stream.on('close', SegmentFetcher.onStreamClose);
        this.#streams.add(stream);

        SegmentFetcher.#trackers.set(stream, this);
    }

    /**
     * @param {NodeJS.ReadStream} stream
     */
    _stopTracking(stream) {

        stream.removeListener('close', SegmentFetcher.onStreamClose);
        this.#streams.delete(stream);
    }

    static onStreamClose() {

        SegmentFetcher.#trackers.get(this)._stopTracking(this);
    }
};
