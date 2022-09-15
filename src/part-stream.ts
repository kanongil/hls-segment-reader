import { AbortablePromise, assert, Deferred, performFetch } from 'hls-playlist-reader/helpers';
import type { Byterange, FetchResult } from 'hls-playlist-reader/helpers';
import { PreloadHints } from 'hls-playlist-reader/playlist';

type ExtendedFetch = AbortablePromise<FetchResult & { part: Part }>;

export interface PartHint {
    uri: string;
    type: 'PART' | 'MAP';
    byterange?: Byterange;
}

type Hint = {
    part: PartHint;
    fetch: AbortablePromise<FetchResult>;
};

export type Part = {
    uri?: string;
    byterange?: Byterange;
    final?: boolean;
    hint?: Hint;
};

interface PartStreamOptions {
    baseUrl: string;
    signal: AbortSignal;
}

class PartStreamImpl<T extends object> {

    #queuedParts: Part[] = [];
    #fetches: ExtendedFetch[] = [];
    #meta = Object.assign(new Deferred<FetchResult['meta']>(), { queued: false });
    #fetchTimer?: NodeJS.Timeout;
    #hint?: Hint;
    #baseUrl: string;
    #signal: AbortSignal;
    #feed: (err?: Error, stream?: T) => Promise<void>;

    constructor(feedFn: (err?: Error, stream?: T) => Promise<void>, { baseUrl, signal }: PartStreamOptions) {

        this.#feed = feedFn;
        this.#baseUrl = baseUrl;
        this.#signal = signal;
    }

    addParts(parts: Part[], final = false) {

        if (!Array.isArray(parts)) {
            throw new TypeError('Parts must be an array');
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

            this._feedFetches(fetches).catch(this.#feed.bind(this));

            this.#queuedParts = [];
        };

        clearTimeout(this.#fetchTimer);
        this.#fetchTimer = setTimeout(start, 0, this.#hint);
        this.#hint = undefined;
    }

    setHint(hint?: PartHint): void {

        if (hint && hint.type !== 'PART') {
            return;         // TODO: support MAP hints
        }

        if (!this._isHinted(hint, this.#hint)) {
            if (this.#hint) {
                this.#hint.fetch.abort();
                this.#hint = undefined;
            }

            if (hint) {
                const fetch = performFetch(new URL(hint.uri, this.#baseUrl), { byterange: hint.byterange, signal: this.#signal });
                fetch.catch(() => undefined);
                this.#hint = { part: hint, fetch };
            }
        }
    }

    cancel(reason?: Error) {

        const fetches = this.#fetches;
        this.#fetches = [];
        for (const fetch of fetches) {
            //fetch.catch(() => undefined);
            fetch.abort();
        }

        this.setHint();
        this.#meta.reject(reason || new Error('destroyed'));
    }

    private _isHinted(part?: Part | PartHint, hint?: Hint) {

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

    meta() {

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

                if (part.byterange.offset === last.byterange.offset + last.byterange.length!) {

                    last.byterange.length! += part.byterange.length!;
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

        let fetch: AbortablePromise<FetchResult>;
        if (part.hint) {
            fetch = part.hint.fetch;
        }
        else {
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

            fetch = performFetch(new URL(part.uri, this.#baseUrl), { byterange: part.byterange, signal: this.#signal });
        }

        return Object.assign(fetch.then((fetchResult) => ({ ...fetchResult, part })), {
            abort: () => fetch.abort()
        });
    }

    async _feedPart(stream: T | undefined, part: Part) {

        // TODO: only feed part.byterange.length in case it is longer??

        await this.#feed(undefined, stream);
        if (part.final) {
            return this.#feed();
        }
    }
}

export class PartStream extends ReadableStream {

    #impl: PartStreamImpl<ReadableStream<Uint8Array>>;

    get meta(): Promise<FetchResult['meta']> {

        return this.#impl.meta();
    }

    constructor(options: PartStreamOptions) {

        super();

        const transform = new TransformStream();

        this.#impl = new PartStreamImpl<ReadableStream<Uint8Array>>((err, stream) => {

            if (err) {
                return transform.writable.abort(err);
            }

            if (stream) {
                return stream.pipeTo(transform.writable, { preventClose: true });
            }

            return transform.writable.close().catch(() => undefined);
        }, options);

        // Mirror transform ReadableStream

        for (const key of Reflect.ownKeys(ReadableStream.prototype)) {
            const descriptor = Object.getOwnPropertyDescriptor(ReadableStream.prototype, key)!;
            if (key === 'constructor') {
                continue;
            }

            if (descriptor.value) {
                descriptor.value = typeof descriptor.value === 'function' ? descriptor.value.bind(transform.readable) : descriptor.value;
            }
            else {
                descriptor.get = descriptor.get?.bind(transform.readable);
            }

            Object.defineProperty(this, key, descriptor);
        }
    }

    append(parts: Part[], final = false): void {

        this.#impl.addParts(parts, final);
    }

    hint(hint?: PreloadHints): void {

        if (!hint) {
            return;
        }

        assert(!hint.map, 'MAP hint is not supported');
        assert(hint.part, 'PART hint is required');

        this.#impl.setHint({ ...hint.part, type: 'PART' });
    }
}
