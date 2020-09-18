import type { MediaPlaylist, MediaSegment } from 'm3u8parse';
export interface DestroyableStream {
    destroy(err?: Error): void;
    destroyed: boolean;
}
export type ReadableStream = NodeJS.ReadableStream & DestroyableStream;

import { watch } from 'fs';
import { basename, dirname } from 'path';
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
    retries?: number;
};

export type PartData = {
    uri: string;
    byterange?: Byterange;
};

type PreloadHints = {
    part?: PartData;
    map?: PartData;
};

export const performFetch = function (uri: URL | string, { byterange, probe = false, timeout, retries = 1 }: FetchOptions = {}): AbortablePromise<FetchResult> {

    const streamOptions = Object.assign({
        probe,
        highWaterMark: internals.fetchBuffer,
        timeout: probe ? 30 * 1000 : timeout,
        retries
    }, byterange ? {
        start: byterange.offset,
        end: byterange.length !== undefined ? byterange.offset + byterange.length - 1 : undefined
    } : undefined);

    const stream = UriStream(uri.toString(), streamOptions);

    const promise = new Promise<FetchResult>((resolve, reject) => {

        const doFinish = (err: Error | null, meta?: Uristream.Meta) => {

            stream.removeListener('meta', onMeta);
            stream.removeListener('end', onFail);
            stream.removeListener('error', onFail);

            if (err || !meta) {
                return reject(err);
            }

            const result = { meta, stream: !probe && stream || undefined };
            if (!result.stream) {
                stream.destroy();
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

    isSameHead(index: MediaPlaylist, includePartial = false): boolean {

        const sameMsn = this.index.lastMsn(includePartial) === index.lastMsn(includePartial);
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
                msn: this.index.lastMsn(true) + +!hasPartialSegment,
                part: hasPartialSegment ? parts.length : 0
            };
        }

        return { msn: this.index.lastMsn(false) + 1 };
    }

    get segments(): MediaSegment[] {

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

    private _watcher: ReturnType<typeof watch>;
    private _last?: FSWatcherEvents;
    private _error?: Error;
    private _deferred?: Deferred<FSWatcherEvents>;

    constructor(uri: URL | string) {

        const change = (eventType: FSWatcherEvents, name: string) => {

            if (name !== fileName) {
                return;
            }

            if (this._deferred) {
                this._deferred.resolve(eventType);
                this._deferred = undefined;
                return;
            }

            this._last = eventType;
        };

        const error = (err: Error) => {

            if (this._deferred) {
                this._deferred.reject(err);
                this._deferred = undefined;
            }

            this._error = err;
        };

        const path = fileURLToPath(uri);
        const fileName = basename(path);
        const dirName = dirname(path);

        // Watch parent dir, since an atomic replace will have a new inode, and stop the watch for the path

        const watcher = this._watcher = watch(dirName, { persistent: false });

        watcher.on('change', change);
        watcher.on('error', error);
        watcher.once('close', () => {

            watcher.removeListener('change', change);
            watcher.removeListener('error', error);
            this._last = undefined;
        });
    }

    // Returns latest event since last call, or waits for next

    next(): PromiseLike<FSWatcherEvents> | FSWatcherEvents {

        if (this._error) {
            throw this._error;
        }

        const last = this._last;
        if (last !== undefined) {
            this._last = undefined;
            return last;
        }

        this._deferred = new Deferred();

        return this._deferred.promise;
    }

    close(): void {

        if (!this._error) {
            this._watcher.close();

            if (this._deferred) {
                this._deferred.reject(new Error('closed'));
                this._deferred = undefined;
            }
        }
    }
}
