import type { Readable, Writable, Transform } from 'readable-stream';

// Fix NodeJS.WritableStream to allow any type chunks

/*declare global {
    namespace NodeJS {
        interface ReadableStream {
            pipe<T extends NodeJS.WritableStream | Writable>(destination: T, options?: { end?: boolean; }): T;
            unpipe(destination?: NodeJS.WritableStream | Writable): this;
        }
        interface WritableStream {
            write(chunk: any, encoding?: BufferEncoding, cb?: (err?: Error | null) => void): boolean;
            end(chunk: any, encoding?: BufferEncoding, cb?: () => void): void;
        }
    }
}*/

/*declare module "stream" {
    class internal {
        pipe<T extends NodeJS.WritableStream | Writable>(destination: T, options?: { end?: boolean; }): T;
    }
}*/

export interface ReadableEvents<T> {
    close: () => void;
    data: (chunk: T) => void;
    end: () => void;
    error: (err: Error) => void;
    pause: () => void;
    readable: () => void;
    resume: () => void;
}

export interface WritableEvents {
    close: () => void;
    drain: () => void;
    error: (err: Error) => void;
    finish: () => void;
    pipe: (src: Readable) => void;
    unpipe: (src: Readable) => void;
}

export interface DuplexEvents<T> extends ReadableEvents<T>, WritableEvents {}

declare type ListenerSignature<L> = {
    [E in keyof L]: (...args: any[]) => any;
};

declare class TypedReadable<T = Buffer, L extends ListenerSignature<L> = ReadableEvents<T>> extends Readable {
    push(chunk: T | null): boolean;
    unshift(chunk: T | null): boolean;
    read(size?: number): T | null;

    addListener<U extends keyof L>(event: U, listener: L[U]): this;
    prependListener<U extends keyof L>(event: U, listener: L[U]): this;
    prependOnceListener<U extends keyof L>(event: U, listener: L[U]): this;
    removeListener<U extends keyof L>(event: U, listener: L[U]): this;
    removeAllListeners(event: keyof L): this;
    removeAllListeners(event?: string | symbol): this;
    once<U extends keyof L>(event: U, listener: L[U]): this;
    on<U extends keyof L>(event: U, listener: L[U]): this;
    off<U extends keyof L>(event: U, listener: L[U]): this;
    off(event: string | symbol, listener: (...args: any[]) => void): this;
    emit<U extends keyof L>(event: U, ...args: Parameters<L[U]>): boolean;
    emit(event: string | symbol, ...args: any[]): boolean;

    pipe<S extends NodeJS.WritableStream | Writable>(destination: S, options?: { end?: boolean }): S;
    unpipe(destination?: NodeJS.WritableStream | Writable): this;
}

declare class TypedWritable<T = Buffer, L extends ListenerSignature<L> = WritableEvents> extends Writable {
    write(chunk: T, cb?: (error: Error | null | undefined) => void): boolean;
    write(chunk: T, encoding: BufferEncoding, cb?: (error: Error | null | undefined) => void): boolean;
    end(cb?: () => void): void;
    end(chunk: T, cb?: () => void): void;
    end(chunk: T, encoding: BufferEncoding, cb?: () => void): void;

    _write(chunk: T, encoding: BufferEncoding, callback: (error?: Error | null) => void): void;
    _writev?(chunks: Array<{ chunk: T; encoding?: BufferEncoding }>, callback: (error?: Error | null) => void): void;

    addListener<U extends keyof L>(event: U, listener: L[U]): this;
    prependListener<U extends keyof L>(event: U, listener: L[U]): this;
    prependOnceListener<U extends keyof L>(event: U, listener: L[U]): this;
    removeListener<U extends keyof L>(event: U, listener: L[U]): this;
    removeAllListeners(event: keyof L): this;
    removeAllListeners(event?: string | symbol): this;
    once<U extends keyof L>(event: U, listener: L[U]): this;
    on<U extends keyof L>(event: U, listener: L[U]): this;
    off<U extends keyof L>(event: U, listener: L[U]): this;
    off(event: string | symbol, listener: (...args: any[]) => void): this;
    emit<U extends keyof L>(event: U, ...args: Parameters<L[U]>): boolean;
    emit(event: string | symbol, ...args: any[]): boolean;
}

declare class TypedTransform<W = Buffer, R = Buffer, L extends ListenerSignature<L> = DuplexEvents<R>> extends Transform {
    push(chunk: R | null): boolean;
    unshift(chunk: R | null): boolean;
    read(size?: number): R | null;

    write(chunk: W, cb?: (error: Error | null | undefined) => void): boolean;
    write(chunk: W, encoding: BufferEncoding, cb?: (error: Error | null | undefined) => void): boolean;
    write(chunk: W, encoding: BufferEncoding, cb?: (error: Error | null | undefined) => void): boolean;
    end(cb?: () => void): void;
    end(chunk: W, cb?: () => void): void;
    end(chunk: W, encoding: BufferEncoding, cb?: () => void): void;

    _transform(chunk: W, encoding: BufferEncoding, callback: (error?: Error, data?: any) => void): void;

    addListener<U extends keyof L>(event: U, listener: L[U]): this;
    addListener(event: string | symbol, listener: (...args: any[]) => void): this;
    prependListener<U extends keyof L>(event: U, listener: L[U]): this;
    prependOnceListener<U extends keyof L>(event: U, listener: L[U]): this;
    removeListener<U extends keyof L>(event: U, listener: L[U]): this;
    removeAllListeners(event: keyof L): this;
    removeAllListeners(event?: string | symbol): this;
    once<U extends keyof L>(event: U, listener: L[U]): this;
    on<U extends keyof L>(event: U, listener: L[U]): this;
    on(event: string | symbol, listener: (...args: any[]) => void): this;
    off<U extends keyof L>(event: U, listener: L[U]): this;
    off(event: string | symbol, listener: (...args: any[]) => void): this;
    emit<U extends keyof L>(event: U, ...args: Parameters<L[U]>): boolean;
    emit(event: string | symbol, ...args: any[]): boolean;

    pipe<T extends NodeJS.WritableStream | Writable>(destination: T, options?: { end?: boolean }): T;
    unpipe(destination?: NodeJS.WritableStream | Writable): this;
}
