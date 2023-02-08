

import { PartStreamOptions, partStreamSetup } from './part-stream.js';

export class PartStream extends partStreamSetup<ReadableStream, typeof ReadableStream<Uint8Array>>(ReadableStream) {

    #transform: TransformStream;

    constructor(options: PartStreamOptions) {

        super(options as any);

        this.#transform = new TransformStream();

        // Mirror transform ReadableStream

        for (const key of Reflect.ownKeys(ReadableStream.prototype)) {
            const descriptor = Object.getOwnPropertyDescriptor(ReadableStream.prototype, key)!;
            if (key === 'constructor') {
                continue;
            }

            if (descriptor.value) {
                descriptor.value = typeof descriptor.value === 'function' ? descriptor.value.bind(this.#transform.readable) : descriptor.value;
            }
            else {
                descriptor.get = descriptor.get?.bind(this.#transform.readable);
            }

            Object.defineProperty(this, key, descriptor);
        }
    }

    _feedPart(err?: Error, stream?: ReadableStream, final?: boolean): Promise<void> | void {

        if (err) {
            return this.#transform.writable.abort(err);
        }

        return stream!.pipeTo(this.#transform.writable, { preventClose: !final });
    }
}
