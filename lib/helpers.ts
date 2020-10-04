import type { Readable } from 'stream';
import type { Meta } from 'uristream/lib/uri-reader';

import { watch } from 'fs';
import { basename, dirname } from 'path';
import { URL, fileURLToPath } from 'url';

import Uristream = require('uristream');


export type Byterange = {
    offset: number;
    length?: number;
};

export type FetchResult = {
    meta: Meta;
    stream?: Readable;
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

    const stream = Uristream(uri.toString(), streamOptions);

    const promise = new Promise<FetchResult>((resolve, reject) => {

        const doFinish = (err: Error | null, meta?: Meta) => {

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

        const onMeta = (meta: Meta) => {

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
