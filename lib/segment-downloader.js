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
 * @property {Required<Helpers.Byterange> | undefined} [byterange]
 * @property {boolean | undefined} [final]
 * @property {Hint | undefined} [hint]
 */

/**
 * @typedef Hint
 * @type {object}
 * @property {{ uri: string, type: 'PART' | 'MAP', byterange?: Helpers.Byterange }} part
 * @property {ExtendedFetch} fetch
 */

/**
 * @typedef ExtendedFetch
 * @type {Promise<Required<Helpers.FetchResult> & { part: Part }> & { abort: () => void }}
 */

class PartStream extends PassThrough {

    /** @type { Part[] } */
    #queuedParts = [];

    /** @type { ExtendedFetch[] } */
    #fetches = [];

    /** @type {Helpers.Deferred<Helpers.FetchResult['meta']> & { queued: boolean }} */
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

        this.#meta = Object.assign(new Helpers.Deferred(), { queued: false });

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

                const fetch = part.hint ? part.hint.fetch : /** @type {ExtendedFetch} */ (Helpers.fetch(part.uri, { byterange: part.byterange }));

                const promise = Object.assign(fetch.then((fetchResult) => {

                    const result = { ...fetchResult, part };

                    if (resolveMeta) {
                        this.#meta.resolve(fetchResult.meta);
                    }

                    return result;
                }), {
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
     * @param {Hint['part']} [hint]
     */
    addHint(hint) {

        if (hint && hint.type !== 'PART') {
            return;         // TODO: support MAP hints
        }

        if (!this._isHinted(hint, this.#hint)) {
            if (this.#hint) {
                this.#hint.fetch.abort();
                this.#hint = undefined;
            }

            if (hint) {
                const fetch = /** @type {ExtendedFetch} */ (Helpers.fetch(hint.uri, { byterange: hint.byterange }));
                fetch.catch(Hoek.ignore);
                this.#hint = { part: hint, fetch };
            }
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
            //fetch.catch(Hoek.ignore);
            fetch.abort();
        }

        this.addHint();
        this.#meta.reject(err || new Error('destroyed'));

        super._destroy(err, cb);
    }

    /**
     * @param {Part | Hint['part'] | undefined} part
     * @param {Hint | undefined} hint
     */
    _isHinted(part, hint) {

        if (!part || !hint) {
            return false;
        }

        if (part.uri !== hint.part.uri) {
            return false;
        }

        // Uris match - check for byterange match

        if (!part.byterange) {
            return !hint.part.byterange;
        }

        if (!hint.part.byterange) {
            return false;
        }

        // Both have byteranges - now check it

        if (part.byterange.offset !== hint.part.byterange.offset) {
            return false;
        }

        return hint.part.byterange.length === undefined ||
               part.byterange.offset === hint.part.byterange.length;
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
                if (this._isHinted(part, hint)) {
                    part.hint = hint;
                    break;
                }
            }
        }

        // Optimization - find common parts + ranges

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
     * @param {Helpers.ReadableStream} stream
     * @param {Part} part
     */
    _feedPart(stream, part) {

        // TODO: only feed part.byterange.length in case it is longer??

        stream.pipe(this, { end: !!part.final });
        return internals.streamFinished(stream);
    }
}


/** @typedef {object | string | number } FetchToken */


module.exports = class SegmentDownloader {

    /** @type {Map<FetchToken, ReturnType<Helpers.fetch>>} */
    #fetches = new Map();

    /**
     * @param {{ probe?: boolean }} options
     */
    constructor(options) {

        options = Hoek.applyToDefaults(internals.defaults, options);

        this.probe = !!options.probe;
    }

    /**
     * @param {FetchToken} token
     * @param {string} uri
     * @param {Required<Helpers.Byterange>} [byterange]
     *
     * @return {ReturnType<Helpers.fetch>}
     */
    fetchSegment(token, uri, byterange) {

        const promise = Helpers.fetch(uri, { byterange, probe: this.probe });
        this._startTracking(token, promise);
        return promise;
    }

    /**
     * @param {FetchToken} token
     * @param {Part[]} parts
     *
     * @return {ReturnType<Helpers.fetch>}
     */
    fetchParts(token, parts, final = false) {

        Hoek.assert(!this.probe, 'Use fetchSegment');

        const stream = new PartStream(parts);
        if (final) {
            stream.addParts([], true);
        }

        /** @type ReturnType<Helpers.fetch> */
        const promise = Object.assign(stream._meta().then((meta) => {

            meta = Object.assign({}, meta, { size: -1 });
            return { meta, stream };
        }), {
            abort() {

                stream.readable && stream.destroy(new Error('aborted'));
            }
        });

        this._startTracking(token, promise);

        return promise;
    }

    /**
     * Stops any fetch not in token list
     *
     * @param {Set<FetchToken>} tokens
     */
    setValid(tokens = new Set()) {

        for (const [token, fetch] of this.#fetches) {

            if (!tokens.has(token)) {
                this._stopTracking(token);
                fetch.abort();
            }
        }
    }

    /**
     * @param {FetchToken} token
     * @param {ReturnType<Helpers.fetch>} promise
     */
    _startTracking(token, promise) {

        Hoek.assert(!this.#fetches.has(token), 'A token can only be tracked once');

        // Setup auto-untracking

        promise.then(({ stream }) => {

            if (!stream) {
                return this._stopTracking(token);
            }

            if (!this.#fetches.has(token)) {
                return;         // It has already been aborted
            }

            Stream.finished(stream, () => this._stopTracking(token));
        }).catch((/*err*/) => {

            this._stopTracking(token);
        });

        this.#fetches.set(token, promise);
    }

    /**
     * @param {FetchToken} token
     */
    _stopTracking(token) {

        this.#fetches.delete(token);
    }
};
