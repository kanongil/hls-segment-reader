/// <reference lib="dom" />

import type * as TAttr from 'm3u8parse/types/attrs';
import type { HlsFetcherObject } from './index.js';
import type { PartStreamCtor } from './part-stream.js';

import { AttrList } from 'm3u8parse';
import { assert, webstreamImpl as WS, Byterange, ContentFetcher, IDownloadTracker, IFetchResult } from 'hls-playlist-reader/helpers';
import { ContentFetcher as ContentFetcherWeb } from 'hls-playlist-reader/helpers.web';

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

    isSameMap(m1?: AttrList<TAttr.Map>, m2?: AttrList<TAttr.Map>) {

        return m1 === m2 || (m1 && m2 && m1.get('uri') === m2.get('uri') && m1.get('byterange') === m2.get('byterange'));
    },

    isAbortedError(err: Error) {

        return err.name === 'AbortError';
    }
};


type StreamTypes = typeof ContentFetcher['StreamProto'] | typeof ContentFetcherWeb['StreamProto'];
type FetchResult = IFetchResult<StreamTypes>;


export class HlsStreamerObject {

    type: 'segment' | 'map';
    file: Omit<FetchResult['meta'], 'etag'>;
    segment: Readonly<HlsFetcherObject>;
    stream?: StreamTypes;

    get attrs(): AttrList<TAttr.Map> | undefined {

        return this.segment.entry.map;
    }

    constructor(fileMeta: FetchResult['meta'], stream: FetchResult['stream'], type: 'segment' | 'map', details: Readonly<HlsFetcherObject>) {

        this.type = type;
        this.file = fileMeta;
        this.stream = stream;
        this.segment = details;
    }
}

export interface HlsSegmentStreamerOptions {
    withData?: boolean; // default true
    highWaterMark?: number;
    streamType?: typeof ContentFetcher['prototype']['type'] | typeof ContentFetcherWeb['prototype']['type'];

    downloadTracker?: IDownloadTracker;

    onProblem?: (err: Error) => void;
}


export class HlsSegmentDataSource implements Transformer<HlsFetcherObject, HlsStreamerObject> {

    static readonly defaultType = new ContentFetcher().type;

    readonly withData: boolean;
    readonly transforms = 0;
    readonly downloadTracker?: IDownloadTracker;

    protected readonly contentFetcher: InstanceType<typeof ContentFetcher | typeof ContentFetcherWeb>;
    protected PartStream: PartStreamCtor<typeof ContentFetcher['StreamProto'] | typeof ContentFetcherWeb['StreamProto']> | undefined;

    #streamType: Required<HlsSegmentStreamerOptions>['streamType'];
    #readState = new (class ReadState {
        indexTokens = new Set<number | string>();
        activeTokens = new Set<number | string>();

        map?: AttrList<TAttr.Map>;
    })();

    #started = false;
    #ended = false;

    constructor(options: Omit<HlsSegmentStreamerOptions, 'highWaterMark'> = {}) {

        this.withData = options.withData ?? true;
        this.downloadTracker = options.downloadTracker;
        this.contentFetcher = undefined as any as HlsSegmentDataSource['contentFetcher'];     // Fake init
        this.#streamType = options.streamType ?? HlsSegmentDataSource.defaultType;

        if (options.onProblem) {
            this.onProblem = options.onProblem;
        }
    }

    async start(_controller: TransformStreamDefaultController): Promise<void> {

        const ContentFetcherImpl = this.#streamType === HlsSegmentDataSource.defaultType ?
            ContentFetcher :
            (await import(`hls-playlist-reader/helpers.${this.#streamType}.js`)).ContentFetcher;

        (<any> this).contentFetcher = new ContentFetcherImpl();

        this.PartStream = (await import(`./part-stream.${this.#streamType}.js`)).PartStream;
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
                    await fetch.completed;       // Note: Since we don't consume the stream, this could potentially deadlock if internal buffer is filled
                    segment.evicted.throwIfAborted();
                }
                catch (err: any) {
                    throw Object.assign(new Error('Failed to download map data: ' + err.message), {
                        httpStatus: err.name !== 'AbortError' ? 500 : undefined
                    });
                }
            }
            catch (err: any) {
                fetch?.cancel();
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

        return new HlsStreamerObject(fetch.meta, fetch.stream, 'map', segment);
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
            fetch?.cancel();
        }
    }

    private async _fetchParts(segment: HlsFetcherObject): Promise<HlsStreamerObject> {

        assert(!(this.contentFetcher instanceof ContentFetcherWeb));     // TODO: support web fetcher

        const partStream = new this.PartStream!(this.contentFetcher, { baseUrl: segment.baseUrl, signal: segment.evicted, tracker: this.downloadTracker });

        const getPartData = (part: AttrList<TAttr.Part>) => ({
            uri: part.get('uri', AttrList.Types.String)!,
            byterange: part.has('byterange') ? Object.assign({ offset: 0 }, part.get('byterange', AttrList.Types.Byterange)) : undefined
        });

        // At this point object.stream has only been readied / opened

        let stream: typeof partStream | undefined = partStream;
        try {

            // Setup update tracker

            segment.onUpdate = function (update, old) {

                const usedParts = old ? old.parts?.length : update.parts?.length;

                assert(update.parts);

                const segmentParts = update.parts.slice(usedParts);
                if (segmentParts.length === 0 && !this.isClosed) {
                    return;      // No new parts
                }

                // FIXME
                partStream.append(segmentParts.map(getPartData), this.isClosed);
                partStream.hint(segment.hints);
            };

            // Prepare parts

            if (segment.entry.parts) {
                partStream.append(segment.entry.parts.map(getPartData));
            }

            partStream.hint(segment.hints!);

            const meta = await partStream.meta;
            assert(!this.#ended, 'ended');

            const obj = new HlsStreamerObject(meta, stream, 'segment', segment);
            stream = undefined;     // Claimed - don't cancel

            this.#started = true;
            return obj;
        }
        finally {
            if (stream) {
                segment.onUpdate = undefined;
                stream.cancel();
            }
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
        return await this.contentFetcher.perform(new URL(uri, baseUrl), { byterange, retries: 2, signal, tracker: this.downloadTracker });
    }
}


export class HlsSegmentStreamer extends WS.ReadableStream<HlsStreamerObject> {

    readonly source: HlsSegmentDataSource;

    constructor(reader: ReadableStream<HlsFetcherObject>, options: HlsSegmentStreamerOptions = {}) {

        super();

        let source;
        const transform = new WS.TransformStream(
            source = new HlsSegmentDataSource(options),
            new WS.CountQueuingStrategy({ highWaterMark: 1 }),
            new WS.CountQueuingStrategy({ highWaterMark: options.highWaterMark ?? 0 })
        );

        this.source = source;

        reader.pipeThrough(transform);

        // TODO: cancel reader, but not streamer on non-Error cancels?

        // Mirror transform ReadableStream

        for (const key of Reflect.ownKeys(WS.ReadableStream.prototype)) {
            const descriptor = Object.getOwnPropertyDescriptor(WS.ReadableStream.prototype, key)!;
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
}
