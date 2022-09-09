/// <reference lib="dom" />

import { assert } from 'hls-playlist-reader/helpers';

import { HlsFetcherObject, HlsSegmentFetcher } from './segment-fetcher.js';


class HlsSegmentSource implements UnderlyingSource<HlsFetcherObject> {

    fetch: HlsSegmentFetcher;

    constructor(fetcher: HlsSegmentFetcher) {

        assert(fetcher instanceof HlsSegmentFetcher, 'A fetcher must be supplied');

        this.fetch = fetcher;
    }

    async start(controller: ReadableStreamDefaultController<HlsFetcherObject>) {

        const index = await this.fetch.start();
        if (index.master) {
            controller.close();
            return;
        }
    }

    async pull(controller: ReadableStreamDefaultController<HlsFetcherObject>) {

        const res = await this.fetch.next();
        if (!res) {
            return controller.close();
        }

        controller.enqueue(res);
    }

    cancel(reason: any) {

        this.fetch?.cancel(reason);
        (<Partial<this>> this).fetch = undefined;      // Unlink reference
    }
}

/**
 * Reads an HLS media playlist, and output segments in order.
 * Live & Event playlists are refreshed as needed, and expired segments are dropped when backpressure is applied.
 */
export class HlsSegmentReadable extends ReadableStream<HlsFetcherObject> {

    source: HlsSegmentSource;

    constructor(fetcher: HlsSegmentFetcher) {

        const source = new HlsSegmentSource(fetcher);

        super(source, new CountQueuingStrategy({ highWaterMark: 0 }));

        this.source = source;
    }
}
