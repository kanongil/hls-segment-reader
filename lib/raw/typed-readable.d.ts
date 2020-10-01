import type { EventEmitter } from 'events';
import type { Readable, Transform/*, Writable*/ } from 'readable-stream';


export interface BaseEvents {
    error(err: Error): void;
}

export interface ReadableEvents<T> extends BaseEvents {
    close(): void;
    data(chunk: T): void;
    end(): void;
    pause(): void;
    readable(): void;
    resume(): void;
}

export interface WritableEvents extends BaseEvents {
    close(): void;
    drain(): void;
    finish(): void;
    pipe(src: Readable): void;
    unpipe(src: Readable): void;
}

export interface DuplexEvents<T> extends ReadableEvents<T>, WritableEvents {}

type ListenerSignature<L> = {
    [E in keyof L]: (...args: any[]) => void;
};

// eslint-disable-next-line @typescript-eslint/ban-types
type Constructor = new (...args: any[]) => {};

interface IEventEmitter<L extends ListenerSignature<L>> {
    addListener<U extends keyof L>(event: U, listener: L[U]): this;
    prependListener<U extends keyof L>(event: U, listener: L[U]): this;
    prependOnceListener<U extends keyof L>(event: U, listener: L[U]): this;
    removeListener<U extends keyof L>(event: U, listener: L[U]): this;
    removeAllListeners(event: keyof L): this;
    once<U extends keyof L>(event: U, listener: L[U]): this;
    on<U extends keyof L>(event: U, listener: L[U]): this;
    off<U extends keyof L>(event: U, listener: L[U]): this;
    emit<U extends keyof L>(event: U, ...args: Parameters<L[U]>): boolean;
}

export declare function TypedEmitter<L extends ListenerSignature<L> = BaseEvents, TBase extends Constructor = typeof EventEmitter>(placeholder?: L, Base?: TBase): {
    new(...args: any[]): IEventEmitter<L>;
} & TBase;

interface ITypedReadable<T> {
    push(chunk: T | null): boolean;
    unshift(chunk: T | null): void;
    read(size?: number): T | null;
    [Symbol.asyncIterator](): AsyncIterableIterator<T>;
}

export declare function TypedReadable<T = Buffer, TBase extends Constructor = typeof Readable>(placeholder?: T, Base?: TBase): {
    new(...args: any[]): ITypedReadable<T>;
} & TBase;

interface ITypedWritable<T> {
    write(chunk: T, cb?: (error: Error | null | undefined) => void): boolean;
    write(chunk: T, encoding: BufferEncoding, cb?: (error: Error | null | undefined) => void): boolean;
    end(cb?: () => void): void;
    end(chunk: T, cb?: () => void): void;
    end(chunk: T, encoding: BufferEncoding, cb?: () => void): void;
}

/*export declare function TypedWritable<T = Buffer, TBase extends Constructor = typeof Writable>(placeholder?: T, Base?: TBase): {
    new(...args: any[]): ITypedWritable<T> & {
        _write(chunk: T, encoding: BufferEncoding, callback: (error?: Error | null) => void): void;
        _writev?(chunks: Array<{ chunk: T; encoding?: BufferEncoding }>, callback: (error?: Error | null) => void): void;
    };
} & TBase;*/

interface ITypedDuplex<W, R> extends ITypedReadable<R>, ITypedWritable<W> {}

export declare function TypedTransform<W = Buffer, R = Buffer, TBase extends Constructor = typeof Transform>(write?: W, read?: R, Base?: TBase): {
    new(...args: any[]): ITypedDuplex<W, R> & {
        _transform(chunk: W, encoding: BufferEncoding, callback: (error?: Error, data?: any) => void): void;
    };
} & TBase;
