import type { AbortablePromise, Byterange, ContentFetcher, IDownloadTracker, IFetchResult } from 'hls-playlist-reader/helpers';
import type { ContentFetcher as ContentFetcherWeb } from 'hls-playlist-reader/helpers.web';

import { AbortError, arrayAt, assert, Deferred } from 'hls-playlist-reader/helpers';
import { PartData, PreloadHints } from 'hls-playlist-reader/playlist';

type FetchResult = IFetchResult<typeof ContentFetcher['StreamProto'] | typeof ContentFetcherWeb['StreamProto']>;

type ExtendedFetch = AbortablePromise<FetchResult> & { part: Part };

type Hint = {
    part: PartData;
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

/**
 * Shared handler to pass part content (and meta) to a stream implementation.
 *
 * Immediately requests all added parts and processes the results in a serial queue.
 *
 * Handles passed hint information to pre-load the future part.
 */
export class PartStreamImpl<T extends object> {

    readonly #contentFetcher: InstanceType<typeof ContentFetcher | typeof ContentFetcherWeb>;
    readonly #baseUrl: string;
    readonly #tracker?: IDownloadTracker;
    readonly #feed: FeedFn<T>;
    readonly #signal: {
        signal: AbortSignal;
        handler: () => void;
    };

    /** Used to internally cancel all active fetches. */
    #ac = new AbortController();
    #fetches: ExtendedFetch[] = [];
    #meta = Object.assign(new Deferred<FetchResult['meta']>(true), { ready: false });
    #hint?: Hint;
    #completed = new Deferred<void>(true);
    #feeding = false;
    //#blocking = Symbol('PartStream');   // We cannot use blocking since the HTTP stack does not handle pipelined requests

    constructor(fetcher: InstanceType<typeof ContentFetcher | typeof ContentFetcherWeb>, feedFn: FeedFn<T>, { baseUrl, signal, tracker }: PartStreamOptions) {

        this.#contentFetcher = fetcher;
        this.#feed = feedFn;
        this.#baseUrl = baseUrl;
        this.#tracker = tracker;

        const handler = () => this.cancel(signal.reason);
        signal.addEventListener('abort', handler, { once: true });

        this.#signal = { signal, handler };
    }

    /** Called once processing is completed to cleanup state */
    #finalize(err?: Error) {

        if (err) {
            (async () => this.#feed(err))().catch((catchedErr) => {

                if (catchedErr !== err) {
                    throw new Error('feedFn failed', { cause: err });
                }
            });

            this.#meta.reject(err);
            this.#ac.abort(err);

            this.#completed.reject(err);
        }

        this.#signal.signal.removeEventListener('abort', this.#signal.handler);
        this.#fetches = [];
        this.#hint = undefined;

        this.#completed.resolve();
    }

    /** Add parts to internal queue */
    addParts(parts?: Part[], hint?: PreloadHints, final = false) {

        assert(!parts || Array.isArray(parts), 'Parts must be an array');

        this.#ac.signal.throwIfAborted();

        const merged = this._mergeParts(parts ?? [], { final });

        // Initiate fetches

        const fetches = merged
            .map((part) => this._fetchPart(part));
        this._fetchHints(final ? undefined : hint);

        // Feed to internal queue and start background feed loop (if not already running)

        this._feedFetches(fetches)
            .catch((err) => this.#finalize(err));     // #finalize() on fetch / feed errors
    }

    cancel(reason?: Error) {

        this.#finalize(new AbortError('Cancelled', { cause: reason }));
    }

    private _isHinted(part?: Part, hint?: Hint) {

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

        this.#fetches.push(...fetches);
        for (const fetch of fetches) {
            fetch.catch(() => undefined);      // No unhandled promise rejection errors
        }

        if (!this.#feeding) {
            await this.#feedAllFetches();
        }
    }

    /** Iterate over the fetches, and pass them to feedFn() in order, as they are ready. */
    async #feedAllFetches() {

        this.#feeding = true;

        try {
            let fetch: ExtendedFetch | undefined;
            while (fetch = this.#fetches.shift()) {
                let res: Awaited<ExtendedFetch>;
                try {
                    res = await fetch;
                }
                catch (err) {
                    // TODO: report problem!?!?

                    if (err instanceof Error && (err.name === 'AbortError' || err.name === 'TimeoutError')) {
                        throw err;
                    }

                    // Cancel all pending & hint fetches to ensure priority for retry request

                    this.#ac.abort(new Error('Pre-empted by retry'));
                    this.#ac = new AbortController();

                    /*const pending = this.#fetches.slice(this.#fetches.indexOf(fetch));
                    for (const item of pending) {
                        // TODO: only cancel if not already completed??
                        item.abort(new Error('Pre-empted by retry'));
                    }

                    this.#hint?.fetch.abort(new Error('Pre-empted by retry'));*/

                    // Immediately retry once

                    fetch = this._fetchPart(fetch.part);

                    // Re-queue remaining parts & hint

                    const parts = this.#fetches.map((entry) => entry.part);
                    const hint = this.#hint ? { part: this.#hint.part } : undefined;
                    const final = !!arrayAt(this.#fetches, -1)?.part.final;
                    this.#fetches = [];
                    this.addParts(parts, hint, final);

                    res = await fetch;
                }

                const { stream, meta } = res;

                if (!this.#meta.ready) {
                    this.#meta.ready = true;
                    this.#meta.resolve(meta);
                }

                // Feed part content to feedFn()

                try {
                    await this.#feed(undefined, stream as T, fetch.part.final === true);
                }
                catch (err) {
                    throw new Error('feedFn failed', { cause: err });
                }

                // The entire part content has now been transferred and consumed.

                if (fetch.part.final) {
                    this.#finalize();
                }
            }
        }
        finally {
            this.#feeding = false;
        }
    }

    meta() {

        return this.#meta.promise;
    }

    get completed(): Promise<void> {

        return this.#completed.promise;
    }

    /** Claim hinted fetch and optimize range requests */
    _mergeParts(parts: Part[], { final }: { final: boolean }): Part[] {

        // Attempt to claim active hint

        const hint = this.#hint;
        if (hint) {
            for (const part of parts) {
                if (this._isHinted(part, hint)) {
                    part.hint = hint;
                    this.#hint = undefined;
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
            part.hint = undefined;
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

                    // eslint-disable-next-line @typescript-eslint/no-empty-function
                    cancel() {},
                    consumeUtf8: () => Promise.resolve('')
                }), { abort: () => undefined, part });
            }

            fetch = this.#contentFetcher.perform(new URL(part.uri, this.#baseUrl), { retries: 0, byterange: part.byterange, signal: this.#ac.signal, tracker: this.#tracker });
        }

        return Object.assign(fetch, {
            part
        });
    }

    _fetchHints(hint?: PreloadHints): void {

        // Cancel unclaimed

        if (this.#hint) {
            this.#hint.fetch.abort();
            this.#hint = undefined;
        }

        // Fetch

        const part = hint?.part;
        if (part) {
            const fetch = this.#contentFetcher.perform(new URL(part.uri, this.#baseUrl), { retries: 0, byterange: part.byterange, signal: this.#ac.signal, tracker: this.#tracker });
            fetch.catch(() => undefined);
            this.#hint = { part, fetch };
        }
    }
}

// eslint-disable-next-line @typescript-eslint/ban-types
type Constructor = new (...args: any[]) => IPartStream;

export interface IPartStream {
    readonly meta: Promise<FetchResult['meta']>;
    readonly completed: Promise<void>;
    append(parts?: PartData[], hint?: PreloadHints, final?: boolean): void;
    abandon(): void;
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

        get completed(): Promise<void> {

            return this.#impl.completed;
        }

        constructor(...args: any[]) {

            super();

            const fetcher = (<ConstructorParameters<PartStreamCtor<any>>> args)[0];
            const options = (<ConstructorParameters<PartStreamCtor<any>>> args)[1];
            this.#impl = new PartStreamImpl<T>(fetcher, this._feedPart.bind(this), options);
        }

        append(parts?: PartData[], hint?: PreloadHints, final = false): void {

            assert(!hint || hint.part, 'Hint must contain a PART hint');
            this.#impl.addParts(parts, hint, final);
        }

        abandon() {

            this.#impl.cancel(new AbortError('Abandoned'));
        }

        _feedPart(_err?: Error, stream?: T, final?: boolean): Promise<void> | void {

            throw new Error('Must be subclassed');
        }
    };
};
