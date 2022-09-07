/// <reference lib="dom" />

import { HlsFetcherObject, HlsSegmentFetcher, HlsSegmentFetcherOptions } from './segment-fetcher.js';


export type HlsSegmentReaderOptions = HlsSegmentFetcherOptions;

class HlsSegmentSource implements UnderlyingSource<HlsFetcherObject> {

    fetch: HlsSegmentFetcher;

    constructor(fetcher: HlsSegmentFetcher) {

        this.fetch = fetcher;
    }

    async start(controller: ReadableStreamDefaultController<HlsFetcherObject>) {

        const index = await this.fetch.start();
        if (index.master) {
            //this.fetch!.cancel();
            controller.close();
            return;
        }
    }

    async pull(controller: ReadableStreamDefaultController<HlsFetcherObject>) {

        const res = await this.fetch.next();
        if (!res) {
            //this.fetch!.cancel();
            return controller.close();
        }

        controller.enqueue(res);
    }

    cancel(reason: any) {

        this.fetch.cancel(reason);
        (<Partial<this>> this).fetch = undefined;      // Unlink reference
    }
}

/**
 * Reads an HLS media playlist, and output segments in order.
 * Live & Event playlists are refreshed as needed, and expired segments are dropped when backpressure is applied.
 */
export class HlsSegmentReadable extends ReadableStream<HlsFetcherObject> {

    fetch: HlsSegmentFetcher;

    constructor(uri: URL | string, options: HlsSegmentReaderOptions = {}) {

        const source = new HlsSegmentSource(
            new HlsSegmentFetcher(uri, options)
        );

        super(source, new CountQueuingStrategy({ highWaterMark: 0 }));

        this.fetch = source.fetch!;
    }
}
