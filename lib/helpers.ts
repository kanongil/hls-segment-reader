import type { MediaPlaylist, M3U8IndependentSegment, M3U8Segment } from 'm3u8parse/lib/m3u8playlist';
export type ReadableStream = NodeJS.ReadableStream & { destroy(err?: Error): void; destroyed: boolean };

import { watch } from 'fs';
import { URL, fileURLToPath } from 'url';

// eslint-disable-next-line @typescript-eslint/no-var-requires
const UriStream = require('uristream') as (uri: string, options: Record<string, unknown>) => ReadableStream;

import { AttrList } from 'm3u8parse';

// eslint-disable-next-line @typescript-eslint/no-namespace
namespace Uristream {
    export type Meta = {
        url: string;
        mime: string;
        size: number;
        modified: number | null;
        etag?: string;
    };
}

export type Byterange = {
    offset: number;
    length?: number;
};

export type FetchResult = {
    meta: Uristream.Meta;
    stream?: ReadableStream;
};


const internals = {
    fetchBuffer: 10 * 1000 * 1000
};


export class Deferred<T> {

    promise: Promise<T>;
    resolve: (arg: T) => void = undefined as any;
    reject: (err: Error) => void = undefined as any;

    constructor() {

        this.promise = new Promise<T>((resolve, reject) => {

            this.resolve = resolve;
            this.reject = reject;
        });
    }
}


type AbortablePromise<T> = Promise<T> & { abort: () => void };

type FetchOptions = {
    byterange?: Byterange;
    probe?: boolean;
    timeout?: number;
};

type PartData = {
    uri: string;
    byterange?: Byterange;
};

type PreloadHints = {
    part?: PartData;
    map?: PartData;
};

export const performFetch = function (uri: URL | string, { byterange, probe = false, timeout }: FetchOptions = {}): AbortablePromise<FetchResult> {

    const streamOptions = Object.assign({
        probe,
        highWaterMark: internals.fetchBuffer,
        timeout: probe ? 30 * 1000 : timeout,
        retries: 1
    }, byterange ? {
        start: byterange.offset,
        end: byterange.length !== undefined ? byterange.offset + byterange.length - 1 : undefined
    } : undefined);

    const stream = UriStream(uri.toString(), streamOptions);

    const promise: ReturnType<typeof performFetch> = new Promise((resolve, reject) => {

        const doFinish = (err: Error | null, meta?: Uristream.Meta) => {

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

        const onMeta = (meta: Uristream.Meta) => {

            meta = Object.assign({}, meta, byterange?.length !== undefined ? { size: byterange.length } : undefined);

            return doFinish(null, meta);
        };

        const onFail = (err?: Error) => {

            if (!err) {
                err = new Error('No metadata');
            }

            return doFinish(err);
        };

        stream.on('meta', onMeta);
        stream.on('end', onFail);
        stream.on('error', onFail);
    }) as any;

    promise.abort = () => !stream.destroyed && stream.destroy(new Error('Aborted'));

    return promise;
};


export class ParsedPlaylist {

    index: MediaPlaylist;

    constructor(index: MediaPlaylist) {

        this.index = index;
    }

    startMsn(full = false): number {

        return this.index.startSeqNo(full);
    }

    lastMsn(includePartial = false): number {

        return this.index.lastSeqNo(includePartial);
    }

    msnForDate(date: Date): number {

        return this.index.seqNoForDate(date, true);
    }

    getResolvedSegment(msn: number, baseUri: URL | string): M3U8IndependentSegment | undefined {

        const segment = this.index.getSegment(msn, true) ?? undefined;
        if (segment) {
            segment.rewriteUris((uri) => {

                return uri !== undefined ? new URL(uri, baseUri).href : uri;
            });
        }

        return segment;
    }

    isSameHead(index: MediaPlaylist, includePartial = false): boolean {

        const sameMsn = this.lastMsn(includePartial) === index.lastSeqNo(includePartial);
        if (!sameMsn || !includePartial) {
            return sameMsn;
        }

        // Same + partial check

        return ((this.segments[this.segments.length - 1].parts || []).length ===
                (index.segments[index.segments.length - 1].parts || []).length);
    }

    nextHead(includePartial = false): { msn: number; part?: number } {

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

    get segments(): M3U8Segment[] {

        return this.index.segments;
    }

    get partTarget(): number | undefined {

        const info = this.index.part_info;
        return info ? info.get('part-target', AttrList.Types.Float) || undefined : undefined;
    }

    get serverControl(): { canBlockReload: boolean; partHoldBack?: number } {

        const control = this.index.server_control;
        return {
            canBlockReload: control ? control.get('can-block-reload') === 'YES' : false,
            partHoldBack: control ? control.get('part-hold-back', AttrList.Types.Float) || undefined : undefined
        };
    }

    get preloadHints(): PreloadHints {

        const hints: PreloadHints = {};

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
}


type FSWatcherEvents = 'rename' | 'change';

export class FsWatcher {

    #watcher: ReturnType<typeof watch>;
    #last?: FSWatcherEvents;
    #error?: Error;
    #deferred?: Deferred<FSWatcherEvents>;

    constructor(uri: URL | string) {

        const change = (eventType: FSWatcherEvents) => {

            if (this.#deferred) {
                this.#deferred.resolve(eventType);
                this.#deferred = undefined;
            }

            this.#last = eventType;
        };

        const error = (err: Error) => {

            if (this.#deferred) {
                this.#deferred.reject(err);
                this.#deferred = undefined;
            }

            this.#error = err;
        };

        const watcher = this.#watcher = watch(fileURLToPath(uri), { persistent: false });

        watcher.on('change', change);
        watcher.on('error', error);
        watcher.once('close', () => {

            watcher.removeListener('change', change);
            watcher.removeListener('error', error);
            this.#last = undefined;
        });
    }

    // Returns latest event since last call, or waits for next

    next(): PromiseLike<FSWatcherEvents> | FSWatcherEvents {

        if (this.#error) {
            throw this.#error;
        }

        const last = this.#last;
        if (last !== undefined) {
            this.#last = undefined;
            return last;
        }

        this.#deferred = new Deferred();

        return this.#deferred.promise;
    }

    close(): void {

        if (!this.#error) {
            this.#watcher.close();

            if (this.#deferred) {
                this.#deferred.reject(new Error('closed'));
                this.#deferred = undefined;
            }
        }
    }
}
