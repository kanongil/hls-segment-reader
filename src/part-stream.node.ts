import { assert, ContentFetcher } from 'hls-playlist-reader/helpers';

import { errorMonitor } from 'node:events';
import { Readable } from 'node:stream';

import { IPartStream, PartStreamCtor, PartStreamOptions, partStreamSetup } from './part-stream.js';

const ignore = () => undefined;

export class PartStream extends partStreamSetup<Readable, Omit<typeof Readable, 'new'> & PartStreamCtor<Readable>>(Readable as any) implements IPartStream {

    #active?: Readable;

    constructor(fetcher: InstanceType<typeof ContentFetcher>, options: PartStreamOptions) {

        super(fetcher, options);

        // Don't hard fail before meta has been resolved

        this.on('error', ignore);
        this.meta.then(() => {

            this.off('error', ignore);

            // Don't hard fail on any unhandled AbortError errors

            this.on(errorMonitor, function (this: PartStream, err) {

                if (err.name === 'AbortError' && this.listenerCount('error') === 0) {
                    this.once('error', () => undefined);
                }
            });
        });
    }

    async #feedPart(stream: Readable | undefined, final: boolean): Promise<void> {

        assert(!this.#active);

        if (stream) {
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
            }
            finally {
                this.#active = undefined;
            }
        }

        if (final) {
            this.push(null);
        }
    }

    _feedPart(err?: Error, stream?: Readable, final?: boolean): Promise<void> | void {

        if (err) {
            this.destroy(err);
            return;
        }

        return this.#feedPart(stream, !!final);
    }

    _read() {

        this.#active?.resume();
    }
}
