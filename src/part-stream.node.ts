import { assert, ContentFetcher } from 'hls-playlist-reader/helpers';

import { Readable } from 'stream';

import { IPartStream, PartStreamCtor, PartStreamOptions, partStreamSetup } from './part-stream.js';

export class PartStream extends partStreamSetup<Readable, Omit<typeof Readable, 'new'> & PartStreamCtor<Readable>>(Readable as any) implements IPartStream {

    #active?: Readable;

    constructor(fetcher: InstanceType<typeof ContentFetcher>, options: PartStreamOptions) {

        super(fetcher, options);

        this.on('error', () => undefined);     // Don't hard fail on unhandled errors
    }

    async #feedPart(stream: Readable, final: boolean): Promise<void> {

        assert(!this.#active);

        try {
            this.#active = stream;

            for await (const chunk of stream) {
                if (this.destroyed) {
                    return;
                }

                if (!this.push(chunk)) {
                    stream.pause();
                }
            }

            if (final) {
                this.push(null);
            }
        }
        finally {
            this.#active = undefined;
        }
    }

    _feedPart(err?: Error, stream?: Readable, final?: boolean): Promise<void> | void {

        if (err) {
            this.destroy(err.name !== 'AbortError' ? err : undefined);
            return;
        }

        if (stream) {
            return this.#feedPart(stream, !!final);
        }
    }

    _read() {

        this.#active?.resume();
    }
}
