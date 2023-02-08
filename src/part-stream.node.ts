
import { Readable, PassThrough, finished } from 'stream';
import { promisify } from 'util';

import { PartStreamOptions, partStreamSetup } from './part-stream.js';

export class PartStream extends partStreamSetup<Readable, typeof PassThrough>(PassThrough) {

    constructor(options: PartStreamOptions) {

        super(options);
    }

    _feedPart(err?: Error, stream?: Readable, final?: boolean): Promise<void> | void {

        if (err) {
            this.destroy(err);
            return;
        }

        stream!.pipe(this, { end: final });

        return promisify(finished)(stream!);
    }
}
