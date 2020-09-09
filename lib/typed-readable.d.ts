import type { Readable } from 'readable-stream';

export interface ReadableEvents<T> {
    close: () => void;
    data: (chunk: T) => void;
    end: () => void;
    error: (err: Error) => void;
    pause: () => void;
    readable: () => void;
    resume: () => void;
}

declare type ListenerSignature<L> = {
    [E in keyof L]: (...args: any[]) => any;
};

declare class TypedReadable<T = Buffer, L extends ListenerSignature<L> = ReadableEvents<T>> extends Readable {
    push<T>(chunk: T | null): boolean;
    unshift<T>(chunk: T | null): boolean;
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
}
