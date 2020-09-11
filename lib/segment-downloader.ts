import { finished } from 'stream';
import { promisify } from 'util';

import { applyToDefaults, assert, ignore } from '@hapi/hoek';
import { PassThrough } from 'readable-stream';

import { Deferred, performFetch } from './helpers';
import type { Byterange, FetchResult, ReadableStream } from './helpers';


const internals = {
    defaults: {
        probe: false
    },
    streamFinished: promisify(finished)
};


type ExtendedFetch = Promise<FetchResult & { part: Part }> & { abort: () => void };

export type Hint = {
    part: { uri: string; type: 'PART' | 'MAP'; byterange?: Byterange };
    fetch: ExtendedFetch;
};

export type Part = {
    uri?: string;
    byterange?: Byterange;
    final?: boolean;
    hint?: Hint;
};


class PartStream extends PassThrough {

    #queuedParts: Part[] = [];
    #fetches: ExtendedFetch[] = [];
    #meta: Deferred<FetchResult['meta']> & { queued: boolean };
    #fetchTimer?: NodeJS.Immediate;
    #hint?: Hint;

    constructor(parts: Part[]) {

        super();

        this.#meta = Object.assign(new Deferred<FetchResult['meta']>(), { queued: false });

        this.addParts(parts);
    }

    addParts(parts: Part[], final = false) {

        if (!Array.isArray(parts)) {
            throw new TypeError('Parts must be an array');
        }

        assert(this.writable, 'Stream cannot be closed');

        if (this.#fetchTimer) {
            clearImmediate(this.#fetchTimer);
        }

        this.#queuedParts.push(...parts);

        const start = (hint?: Hint): void => {

            const fetches = this._mergeParts(this.#queuedParts, { final, hint }).map((part) => {

                const fetch = this._fetchPart(part);

                if (!this.#meta.queued) {
                    this.#meta.queued = false;
                    fetch.then(({ meta }) => this.#meta.resolve(meta), this.#meta.reject);
                }

                return fetch;
            });

            this._feedFetches(fetches).catch((err) => {

                if (!this.destroyed) {
                    this.destroy(err);
                }
            });

            this.#queuedParts = [];
        };

        this.#fetchTimer = setImmediate(start, this.#hint);
        this.#hint = undefined;
    }

    addHint(hint?: Hint['part']): void {

        if (hint && hint.type !== 'PART') {
            return;         // TODO: support MAP hints
        }

        if (!this._isHinted(hint, this.#hint)) {
            if (this.#hint) {
                this.#hint.fetch.abort();
                this.#hint = undefined;
            }

            if (hint) {
                const fetch = performFetch(hint.uri, { byterange: hint.byterange }) as ExtendedFetch;
                fetch.catch(ignore);
                this.#hint = { part: hint, fetch };
            }
        }
    }

    /*protected*/ _destroy(err: Error | null, cb: any) {

        const fetches = this.#fetches;
        this.#fetches = [];
        for (const fetch of fetches) {
            //fetch.catch(ignore);
            fetch.abort();
        }

        this.addHint();
        this.#meta.reject(err || new Error('destroyed'));

        super._destroy(err, cb);
    }

    private _isHinted(part?: Part | Hint['part'], hint?: Hint) {

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

    async _feedFetches(fetches: ExtendedFetch[]) {

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

    _mergeParts(parts: Part[], { final, hint }: { final: boolean; hint?: Hint }): Part[] {

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
            if (!merged.length) {
                merged.push({});
            }

            merged[merged.length - 1].final = true;
        }

        return merged;
    }

    _fetchPart(part: Part): ExtendedFetch {

        if (part.hint) {
            return part.hint.fetch;
        }

        if (!part.uri) {
            return Object.assign(Promise.resolve({
                stream: undefined,
                meta: {
                    url: '',
                    mime: '',
                    size: -1,
                    modified: null
                },
                part
            }), { abort: () => undefined });
        }

        const fetch = performFetch(part.uri, { byterange: part.byterange });

        return Object.assign(fetch.then((fetchResult) => ({ ...fetchResult, part })), {
            abort: () => fetch.abort()
        });
    }

    _feedPart(stream: ReadableStream | undefined, part: Part) {

        // TODO: only feed part.byterange.length in case it is longer??

        if (!stream) {
            if (part.final) {
                this.push(null);
            }

            return;
        }

        stream.pipe(this, { end: !!part.final });
        return internals.streamFinished(stream);
    }
}


// eslint-disable-next-line @typescript-eslint/ban-types
type FetchToken = object | string | number;

export class SegmentDownloader {

    probe: boolean;

    #fetches = new Map<FetchToken, ReturnType<typeof performFetch>>();

    constructor(options: { probe?: boolean }) {

        options = applyToDefaults(internals.defaults, options);

        this.probe = !!options.probe;
    }

    fetchSegment(token: FetchToken, uri: string, byterange?: Required<Byterange>): ReturnType<typeof performFetch> {

        const promise = performFetch(uri, { byterange, probe: this.probe });
        this._startTracking(token, promise);
        return promise;
    }

    fetchParts(token: FetchToken, parts: Part[], final = false): ReturnType<typeof performFetch> {

        assert(!this.probe, 'Use fetchSegment');

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
    setValid(tokens = new Set()): void {

        for (const [token, fetch] of this.#fetches) {

            if (!tokens.has(token)) {
                this._stopTracking(token);
                fetch.abort();
            }
        }
    }

    private _startTracking(token: FetchToken, promise: ReturnType<typeof performFetch>) {

        assert(!this.#fetches.has(token), 'A token can only be tracked once');

        // Setup auto-untracking

        promise.then(({ stream }) => {

            if (!stream) {
                return this._stopTracking(token);
            }

            if (!this.#fetches.has(token)) {
                return;         // It has already been aborted
            }

            finished(stream, () => this._stopTracking(token));
        }).catch((/*err*/) => {

            this._stopTracking(token);
        });

        this.#fetches.set(token, promise);
    }

    private _stopTracking(token: FetchToken) {

        this.#fetches.delete(token);
    }
}
