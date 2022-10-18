/// <reference lib="dom" />

import type { HlsFetcherObject } from './index.js';
import { Byterange, cancelFetch, IDownloadTracker, performFetch } from 'hls-playlist-reader/helpers';

import { AttrList } from 'm3u8parse';
import { assert } from 'hls-playlist-reader/helpers';
import { PartStream } from './part-stream.js';

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


type FetchResult = Awaited<ReturnType<typeof performFetch>>;


export class HlsStreamerObject {

    type: 'segment' | 'map';
    file: FetchResult['meta'];
    stream?: FetchResult['stream'];
    segment?: HlsFetcherObject;
    attrs?: AttrList;

    constructor(fileMeta: FetchResult['meta'], stream: FetchResult['stream'], type: 'map', details: AttrList);
    constructor(fileMeta: FetchResult['meta'], stream: FetchResult['stream'], type: 'segment', details: HlsFetcherObject);

    constructor(fileMeta: FetchResult['meta'], stream: FetchResult['stream'], type: 'segment' | 'map', details: HlsFetcherObject | AttrList) {

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

export interface HlsSegmentStreamerOptions {
    withData?: boolean; // default true
    highWaterMark?: number;

    downloadTracker?: IDownloadTracker;

    onProblem?: (err: Error) => void;
}


export class HlsSegmentDataSource implements Transformer<HlsFetcherObject, HlsStreamerObject> {

    readonly withData: boolean;
    readonly transforms = 0;
    readonly downloadTracker?: IDownloadTracker;

    #readState = new (class ReadState {
        indexTokens = new Set<number | string>();
        activeTokens = new Set<number | string>();

        map?: AttrList;
    })();

    #started = false;
    #ended = false;

    constructor(options: Omit<HlsSegmentStreamerOptions, 'highWaterMark'> = {}) {

        this.withData = options.withData ?? true;
        this.downloadTracker = options.downloadTracker;

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

    flush(_controller: TransformStreamDefaultController) {

        this.#ended = true;
    }

    get segmentMimeTypes(): Set<string> {

        return internals.segmentMimeTypes;
    }

    validateSegmentMeta(meta: FetchResult['meta']): void | never {

        // Check for valid mime type

        if (!this.segmentMimeTypes.has(meta.mime.toLowerCase())) {
            throw new Error(`Unsupported segment MIME type: ${meta.mime}`);         // FIXME: throwing here causes uncought AbortError?!?!
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
        do {
            try {
                tries++;
                fetch = await this._fetchFrom({ uri, byterange }, { baseUrl: segment.baseUrl, signal: segment.evicted });
                assert(fetch.stream);

                try {
                    this.validateSegmentMeta(fetch.meta);
                }
                catch (err) {
                    tries = Infinity;  // Don't retry
                    throw err;
                }

                // Fully buffer stream to ensure it goes well

                try {
                    await fetch.completed;
                    segment.evicted.throwIfAborted();
                }
                catch (err: any) {
                    throw Object.assign(new Error('Failed to download map data: ' + err.message), {
                        httpStatus: err.name !== 'AbortError' ? 500 : undefined
                    });
                }
            }
            catch (err: any) {
                cancelFetch(fetch);
                fetch = undefined;

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
        } while (!fetch);

        assert(!this.#ended, 'ended');

        return new HlsStreamerObject(fetch.meta, fetch.stream, 'map', segment.entry.map);
    }

    private async _fetchSegment(segment: HlsFetcherObject): Promise<HlsStreamerObject> {

        let fetch: FetchResult | undefined = await this._fetchFrom({ uri: segment.entry.uri!, byterange: segment.entry.byterange }, { baseUrl: segment.baseUrl, signal: segment.evicted });

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

            const obj = new HlsStreamerObject(fetch.meta, fetch.stream, 'segment', segment);
            fetch = undefined;     // Claimed - don't cancel

            this.#started = true;
            return obj;
        }
        finally {
            cancelFetch(fetch);
        }
    }

    private async _fetchParts(segment: HlsFetcherObject): Promise<HlsStreamerObject> {

        let stream: PartStream | undefined = new PartStream({ baseUrl: segment.baseUrl, signal: segment.evicted, tracker: this.downloadTracker });

        const getPartData = (part: AttrList) => ({
            uri: part.get('uri', AttrList.Types.String)!,
            byterange: part.has('byterange') ? Object.assign({ offset: 0 }, part.get('byterange', AttrList.Types.Byterange)) : undefined
        });

        // At this point object.stream has only been readied / opened

        try {

            // Setup update tracker

            const updateStream = stream;
            segment.onUpdate = function (update, old) {

                const usedParts = old ? old.parts?.length : update.parts?.length;

                assert(update.parts);

                const segmentParts = update.parts.slice(usedParts);
                if (segmentParts.length === 0 && !this.isClosed) {
                    return;      // No new parts
                }

                // FIXME
                updateStream.append(segmentParts.map(getPartData), this.isClosed);
                updateStream.hint(segment.hints);
            };

            // Prepare parts

            assert(segment.entry.parts);

            stream.append(segment.entry.parts.map(getPartData));
            stream.hint(segment.hints!);

            const meta = await stream.meta;
            assert(!this.#ended, 'ended');

            const obj = new HlsStreamerObject(meta, stream, 'segment', segment);
            stream = undefined;     // Claimed - don't cancel

            this.#started = true;
            return obj;
        }
        finally {
            stream?.cancel();
        }
    }

    private async _process(segment: HlsFetcherObject, controller: TransformStreamDefaultController): Promise<void> {

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

        let obj: HlsStreamerObject;
        if (segment.entry.isPartial()) {
            obj = await this._fetchParts(segment);
        }
        else {
            await segment.closed();

            /* if (segment.entry.gap) {

                // Create an empty object

                obj = new HlsStreamerObject({
                    url: new URL(segment.entry.uri!, segment.baseUrl).href,
                    mime: 'application/octet-stream',
                    size: -1,
                    modified: null
                }, undefined, 'segment', segment);
            }
            else*/ {
                obj = await this._fetchSegment(segment);
            }
        }

        await map;      // Ensure that any init segment has been enqueued
        controller.enqueue(obj);
    }

    private async _fetchFrom(entry: { uri: string; byterange?: Required<Byterange> }, { baseUrl, signal }: { baseUrl: string; signal: AbortSignal }) {

        const { uri, byterange } = entry;
        return await performFetch(new URL(uri, baseUrl), { byterange, retries: 2, signal, tracker: this.downloadTracker });
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

        reader.pipeThrough(transform);

        // TODO: cancel reader, but not streamer on non-Error cancels?

        // Mirror transform ReadableStream

        for (const key of Reflect.ownKeys(ReadableStream.prototype)) {
            const descriptor = Object.getOwnPropertyDescriptor(ReadableStream.prototype, key)!;
            if (key === 'constructor') {
                continue;
            }

            if (descriptor.value) {
                descriptor.value = typeof descriptor.value === 'function' ? descriptor.value.bind(transform.readable) : descriptor.value;
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
