import type { ContentFetcher } from 'hls-playlist-reader/helpers.web';

import { webstreamImpl as WS } from 'hls-playlist-reader/helpers';

import { PartStreamCtor, PartStreamOptions, partStreamSetup } from './part-stream.js';

export class PartStream extends partStreamSetup<ReadableStream, Omit<ReadableStream<Uint8Array>, 'new'> & PartStreamCtor<ReadableStream>>(WS.ReadableStream as any) {

    #transform: TransformStream;

    constructor(fetcher: InstanceType<typeof ContentFetcher>, options: PartStreamOptions) {

        super(fetcher, options);

        this.#transform = new WS.TransformStream();

        // Mirror transform ReadableStream

        for (const key of Reflect.ownKeys(WS.ReadableStream.prototype)) {
            const descriptor = Object.getOwnPropertyDescriptor(WS.ReadableStream.prototype, key)!;
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

        if (stream) {
            return stream!.pipeTo(this.#transform.writable, { preventClose: !final });
        }

        if (final) {
            return this.#transform.writable.close();
        }
    }
}
