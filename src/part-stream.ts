import { AbortablePromise, arrayAt, Byterange, ContentFetcher, IDownloadTracker, IFetchResult } from 'hls-playlist-reader/helpers';
import type { ContentFetcher as ContentFetcherWeb } from 'hls-playlist-reader/helpers.web';

import { assert, Deferred } from 'hls-playlist-reader/helpers';
import { PreloadHints } from 'hls-playlist-reader/playlist';

type FetchResult = IFetchResult<typeof ContentFetcher['StreamProto'] | typeof ContentFetcherWeb['StreamProto']>;

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

export interface PartStreamOptions {
    baseUrl: string;
    signal: AbortSignal;
    tracker?: IDownloadTracker;
}

type FeedFn<T> = (err?: Error, stream?: T, final?: boolean) => Promise<void> | void;

export class PartStreamImpl<T extends object> {

    readonly #contentFetcher: InstanceType<typeof ContentFetcher | typeof ContentFetcherWeb>;
    readonly #baseUrl: string;
    readonly #signal: AbortSignal;
    readonly #tracker?: IDownloadTracker;
    readonly #feed: FeedFn<T>;

    #queuedParts: Part[] = [];
    #fetches: ExtendedFetch[] = [];
    #meta = Object.assign(new Deferred<FetchResult['meta']>(), { queued: false });
    #fetchTimer?: ReturnType<typeof setTimeout>;
    #hint?: Hint;

    constructor(fetcher: InstanceType<typeof ContentFetcher | typeof ContentFetcherWeb>, feedFn: FeedFn<T>, { baseUrl, signal, tracker }: PartStreamOptions) {

        this.#contentFetcher = fetcher;
        this.#feed = feedFn;
        this.#baseUrl = baseUrl;
        this.#signal = signal;
        this.#tracker = tracker;
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
                const blocking = 'hint+' + this.#baseUrl;
                const fetch = this.#contentFetcher.perform(new URL(hint.uri, this.#baseUrl), { byterange: hint.byterange, signal: this.#signal, tracker: this.#tracker, blocking });
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
                await this._feedPart(stream as T, part);
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

            arrayAt(merged, -1)!.final = true;
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

                // TODO: just throw?

                return Object.assign(Promise.resolve({
                    stream: undefined,
                    meta: {
                        url: '',
                        mime: '',
                        size: -1,
                        modified: null
                    },
                    completed: Promise.resolve(),
                    part,

                    // eslint-disable-next-line @typescript-eslint/no-empty-function
                    cancel() {},
                    consumeUtf8: () => Promise.resolve('')
                }), { abort: () => undefined });
            }

            fetch = this.#contentFetcher.perform(new URL(part.uri, this.#baseUrl), { byterange: part.byterange, signal: this.#signal, tracker: this.#tracker });
        }

        return Object.assign(fetch.then((fetchResult) => ({ ...fetchResult, part })), {
            abort: () => fetch.abort()
        });
    }

    async _feedPart(stream: T | undefined, part: Part) {

        // TODO: only feed part.byterange.length in case it is longer??

        await this.#feed(undefined, stream, part.final === true);
    }
}

// eslint-disable-next-line @typescript-eslint/ban-types
type Constructor = new (...args: any[]) => {};

export interface IPartStream {
    readonly meta: Promise<FetchResult['meta']>;
    append(parts: Part[], final?: boolean): void;
    hint(hint?: PreloadHints): void;
    cancel(reason?: Error): void;
}



export interface PartStreamCtor<TBase> {
    new(fetcher: InstanceType<typeof ContentFetcher | typeof ContentFetcherWeb>, options: PartStreamOptions): TBase & IPartStream;
}

export const partStreamSetup = function <T extends object, TBase extends Constructor>(Base: TBase) {

    return class PartStream extends Base {

        #impl: PartStreamImpl<T> = {} as any as PartStreamImpl<T>;

        get meta(): Promise<FetchResult['meta']> {

            return this.#impl.meta().then((meta) => ({
                ...meta,
                size: -1     // Size only represents the first part, not entire segment which is unknown
            }));
        }

        constructor(...args: any[]) {

            super();

            const fetcher = (<ConstructorParameters<PartStreamCtor<any>>> args)[0];
            const options = (<ConstructorParameters<PartStreamCtor<any>>> args)[1];
            this.#impl = new PartStreamImpl<T>(fetcher, this._feedPart.bind(this), options);
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

        _feedPart(_err?: Error, stream?: T, final?: boolean): Promise<void> | void {

            throw new Error('Must be subclassed');
        }
    };
};
