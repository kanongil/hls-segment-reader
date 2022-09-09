/// <reference lib="dom" />

import type { HlsFetcherObject } from './index.js';
import { FetchResult, Byterange, performFetch } from 'hls-playlist-reader/helpers';

import { AttrList } from 'm3u8parse';
import { assert } from 'hls-playlist-reader/helpers';

import { HlsSegmentReadable } from './index.js';
import { HlsSegmentFetcher } from './segment-fetcher.js';

try {
    // TODO: find better way to hook these

    const { types: MimeTypes } = await import('mime' + '-types');

    /* eslint-disable @typescript-eslint/dot-notation */
    MimeTypes['ac3'] = 'audio/ac3';
    MimeTypes['eac3'] = 'audio/eac3';
    MimeTypes['m4s'] = 'video/iso.segment';
    /* eslint-enable @typescript-eslint/dot-notation */
}
catch {}

const internals = {
    segmentMimeTypes: new Set([
        'video/mp2t',
        'video/mpeg',
        'video/mp4',
        'video/iso.segment',
        'video/x-m4v',
        'audio/aac',
        'audio/x-aac',
        'audio/ac3',
        'audio/vnd.dolby.dd-raw',
        'audio/x-ac3',
        'audio/eac3',
        'audio/mp4',
        'text/vtt',
        'application/mp4'
    ]),

    isSameMap(m1?: AttrList, m2?: AttrList) {

        return m1 === m2 || (m1 && m2 && m1.get('uri') === m2.get('uri') && m1.get('byterange') === m2.get('byterange'));
    },

    isAbortedError(err: Error) {

        return err.name === 'AbortError';
    }
};


export class HlsStreamerObject {

    type: 'segment' | 'map';
    file: FetchResult['meta'];
    stream?: ReadableStream<Uint8Array>;
    segment?: HlsFetcherObject;
    attrs?: AttrList;

    constructor(fileMeta: FetchResult['meta'], stream: ReadableStream | undefined, type: 'map', details: AttrList);
    constructor(fileMeta: FetchResult['meta'], stream: ReadableStream | undefined, type: 'segment', details: HlsFetcherObject);

    constructor(fileMeta: FetchResult['meta'], stream: ReadableStream | undefined, type: 'segment' | 'map', details: HlsFetcherObject | AttrList) {

        const isSegment = type === 'segment';

        this.type = type;
        this.file = fileMeta;
        this.stream = stream;

        if (isSegment) {
            this.segment = details as HlsFetcherObject;
        }
        else {
            this.attrs = details as AttrList;
        }
    }
}

export type HlsSegmentStreamerOptions = {
    withData?: boolean; // default true
    highWaterMark?: number;

    onProblem?: (err: Error) => void;
};


export class HlsSegmentDataSource implements Transformer<HlsFetcherObject, HlsStreamerObject> {

    readonly withData: boolean;
    readonly transforms = 0;

    #readState = new (class ReadState {
        indexTokens = new Set<number | string>();
        activeTokens = new Set<number | string>();

        map?: AttrList;
    })();

    #started = false;
    #ended = false;

    constructor(options: Omit<HlsSegmentStreamerOptions, 'highWaterMark'> = {}) {

        this.withData = options.withData ?? true;

        if (options.onProblem) {
            this.onProblem = options.onProblem;
        }
    }

    start(_controller: TransformStreamDefaultController) {

        // Do nothing
    }

    async transform(chunk: HlsFetcherObject, controller: TransformStreamDefaultController) {

        ++(<{ transforms: number }> this).transforms;
        try {
            await this._process(chunk, controller);
        }
        catch (err: any) {
            if (!internals.isAbortedError(err)) {
                throw err;
            }
        }
    }

    flush(controller: TransformStreamDefaultController) {

        this.#ended = true;
    }

    get segmentMimeTypes(): Set<string> {

        return internals.segmentMimeTypes;
    }

    validateSegmentMeta(meta: FetchResult['meta']): void | never {

        // Check for valid mime type

        if (!this.segmentMimeTypes.has(meta.mime.toLowerCase())) {
            throw new Error(`Unsupported segment MIME type: ${meta.mime}`);
        }
    }

    // eslint-disable-next-line @typescript-eslint/no-empty-function
    protected onProblem(_err: Error) {}

    // Private methods

    private async _fetchMapObject(segment: Readonly<HlsFetcherObject>): Promise<HlsStreamerObject> {

        assert(segment.entry.map);

        // Fetch init segment

        const uri = segment.entry.map.get('uri', AttrList.Types.String);
        assert(uri, 'EXT-X-MAP must have URI attribute');
        let byterange: Required<Byterange> | undefined;
        if (segment.entry.map.has('byterange')) {
            byterange = Object.assign({ offset: 0 }, segment.entry.map.get('byterange', AttrList.Types.Byterange)!);
        }

        // Fetching the map is essential to the processing

        let fetch: FetchResult | undefined;
        let tries = 0;
        let forward: ReadableStream | undefined;
        do {
            try {
                tries++;
                fetch = await this._fetchFrom({ uri, byterange }, { baseUrl: segment.baseUrl, signal: segment.evicted });
                assert(fetch.stream);

                const [main, preload] = fetch.stream.tee();
                forward = main;

                // Fully read a tee'ed stream to ensure it goes well

                const reader = preload.getReader();
                try {
                    for (; ;) {
                        const { done } = await reader.read();
                        assert(!this.#ended, 'ended');
                        if (done) {
                            break;
                        }
                    }
                }
                catch (err: any) {
                    reader.cancel();
                    throw Object.assign(new Error('Failed to download map data: ' + err.message), {
                        httpStatus: err.name !== 'AbortError' ? 500 : undefined
                    });
                }
                finally {
                    reader.releaseLock();
                }
            }
            catch (err: any) {
                forward?.cancel();
                segment.evicted.throwIfAborted();

                if (tries >= 4) {
                    throw err;
                }

                this.onProblem(Object.assign(new Error('Failed attempt to fetch map: ' + err.message), {
                    httpStatus: err.name !== 'AbortError' ? err.httpStatus ?? 500 : undefined
                }));

                // Delay and retry

                await new Promise((resolve) => setTimeout(resolve, 200 * (segment.entry.duration || 4)));
                assert(!this.#ended, 'ended');
            }
        } while (!(fetch && forward));

        assert(!this.#ended, 'ended');

        return new HlsStreamerObject(fetch.meta, forward, 'map', segment.entry.map);
    }

    private async _fetchSegment(segment: HlsFetcherObject): Promise<HlsStreamerObject> {

        const fetch = await this._fetchFrom({ uri: segment.entry.uri!, byterange: segment.entry.byterange }, { baseUrl: segment.baseUrl, signal: segment.evicted });
        let stream = fetch.stream;

        // At this point object.stream has only been readied / opened

        try {
            assert(!this.#ended, 'ended');

            // Check meta

            /*if (this.reader && fetch.meta.modified) {
                const segmentTime = segment.entry.program_time || new Date(+fetch.meta.modified - (segment.entry.duration || 0) * 1000);

                if (!this.#started && this.reader.fetch.startDate &&
                    segmentTime < this.reader.fetch.startDate) {

                    fetch.abort();
                    return;   // Too early - ignore segment
                }

                if (this.reader.fetch.stopDate &&
                    segmentTime > this.reader.fetch.stopDate) {

                    fetch.abort();
                    controller.terminate();
                    return;
                }
            }*/

            const obj = new HlsStreamerObject(fetch.meta, stream, 'segment', segment);
            stream = undefined;     // Claimed - don't cancel

            this.#started = true;
            return obj;
        }
        finally {
            stream?.cancel();
        }
    }

    private async _process(segment: HlsFetcherObject, controller: TransformStreamDefaultController): Promise<undefined> {

        let map: Promise<void> | undefined = undefined;

        // Check for new map entry (in the background)

        if (!internals.isSameMap(segment.entry.map, this.#readState.map)) {
            if (segment.entry.map) {
                map = this._fetchMapObject(segment)
                    .then((obj) => {

                        this.#readState.map = segment.entry.map;
                        controller.enqueue(obj);     // Immediately enqueue
                    });
                map.catch(() => undefined);                      // No unhandled promise rejections
            }
        }

        // Fetch the segment

        await segment.closed();
        if (segment.entry.isPartial()) {
            return;
        }

        const obj = await this._fetchSegment(segment);

        await map;      // Ensure that any init segment has been enqueued
        controller.enqueue(obj);
    }

    private async _fetchFrom(entry: { uri: string; byterange?: Required<Byterange> }, { baseUrl, signal }: { baseUrl: string; signal: AbortSignal }) {

        const { uri, byterange } = entry;
        const fetch = await performFetch(new URL(uri, baseUrl), { byterange, retries: 2, signal });

        try {
            this.validateSegmentMeta(fetch.meta);

            return fetch;
        }
        catch (err) {
            fetch.stream?.cancel();

            throw err;
        }
    }
}


export class HlsSegmentStreamer extends ReadableStream<HlsStreamerObject> {

    readonly source: HlsSegmentDataSource;

    constructor(reader: ReadableStream<HlsFetcherObject>, options: HlsSegmentStreamerOptions = {}) {

        super();

        let source;
        const transform = new TransformStream(
            source = new HlsSegmentDataSource(options),
            new CountQueuingStrategy({ highWaterMark: 1 }),
            new CountQueuingStrategy({ highWaterMark: options.highWaterMark ?? 0 })
        );

        this.source = source;

        reader.pipeThrough(transform, {});

        // TODO: cancel reader, but not streamer on non-Error cancels?

        // Mirror transform ReadableStream

        for (const key of Reflect.ownKeys(ReadableStream.prototype)) {
            const descriptor = Object.getOwnPropertyDescriptor(ReadableStream.prototype, key)!;
            if (key === 'constructor') {
                continue;
            }

            if (descriptor.value) {
                descriptor.value = descriptor.value.bind(transform.readable);
            }
            else {
                descriptor.get = descriptor.get?.bind(transform.readable);
            }

            Object.defineProperty(this, key, descriptor);
        }
    }

    /*cancel(reason?: any): Promise<void> {

        if (this.)
    }*/
}
