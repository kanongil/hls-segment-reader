import type { ContentFetcher } from 'hls-playlist-reader/helpers';

import { Readable, PassThrough, finished } from 'stream';
import { promisify } from 'util';

import { IPartStream, PartStreamCtor, PartStreamOptions, partStreamSetup } from './part-stream.js';

export class PartStream extends partStreamSetup<Readable, Omit<typeof PassThrough, 'new'> & PartStreamCtor<PassThrough>>(PassThrough as any) implements IPartStream {

    constructor(fetcher: InstanceType<typeof ContentFetcher>, options: PartStreamOptions) {

        super(fetcher, options);
    }

    _feedPart(err?: Error, stream?: Readable, final?: boolean): Promise<void> | void {

        if (err) {
            this.destroy(err);
            return;
        }

        if (stream) {
            stream.pipe(this, { end: final });

            return promisify(finished)(stream);
        }
    }

    cancel(reason?: Error) {

        this.destroy(reason);
    }
}
