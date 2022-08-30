
import type { Readable } from 'stream';
import type { MediaPlaylist } from 'm3u8parse';
import type { HlsSegmentReadable, HlsFetcherObject } from '.';
import type { FetchResult, Byterange } from 'hls-playlist-reader/lib/helpers';

import { finished } from 'stream';

import { AttrList } from 'm3u8parse';
import { types as MimeTypes } from 'mime-types';
import { assert } from 'hls-playlist-reader/lib/helpers';

import { SegmentDownloader } from './segment-downloader';


/* eslint-disable @typescript-eslint/dot-notation */
MimeTypes['ac3'] = 'audio/ac3';
MimeTypes['eac3'] = 'audio/eac3';
MimeTypes['m4s'] = 'video/iso.segment';
/* eslint-enable @typescript-eslint/dot-notation */


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
    stream?: Readable;
    segment?: HlsFetcherObject;
    attrs?: AttrList;

    constructor(fileMeta: FetchResult['meta'], stream: Readable | undefined, type: 'map', details: AttrList);
    constructor(fileMeta: FetchResult['meta'], stream: Readable | undefined, type: 'segment', details: HlsFetcherObject);

    constructor(fileMeta: FetchResult['meta'], stream: Readable | undefined, type: 'segment' | 'map', details: HlsFetcherObject | AttrList) {

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
};


export class HlsSegmentDataSource implements Transformer<HlsFetcherObject, HlsStreamerObject> {

    baseUrl = 'unknown:';
    readonly withData: boolean;

    #readState = new (class ReadState {
        indexTokens = new Set<number | string>();
        activeTokens = new Set<number | string>();

        //mapSeq: -1,
        map?: AttrList;
        fetching: Promise<HlsStreamerObject> | null = null;
        //active: false;
        //discont: false;
    })();

    #active = new Map<number, FetchResult>(); // used to stop buffering on expired segments
    #downloader: SegmentDownloader;
    reader: HlsSegmentReadable;
    #started = false;

    readonly transforms = 0;
    destroyed = false; // TODO: fix

    constructor(reader: HlsSegmentReadable, options: HlsSegmentStreamerOptions = {}) {

        this.withData = options.withData ?? true;

        this.#downloader = new SegmentDownloader({ probe: !this.withData });
        this.reader = reader;
    }

    start(controller: TransformStreamDefaultController) {

        return this.reader.fetch.source.index().then(({ index, meta }) => {

            assert(!index?.master, 'Source cannot be based on a master playlist');

            this.baseUrl = meta.url;
        });
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

        this.destroyed = true;
    }


    /*abort(graceful = false): void {

        if (!graceful) {
            this.#downloader.setValid();
        }

        if (!this.readable) {
            return;
        }

        this.push(null);
    }*/

    get segmentMimeTypes(): Set<string> {

        return internals.segmentMimeTypes;
    }

    validateSegmentMeta(meta: FetchResult['meta']): void | never {

        // Check for valid mime type

        if (!this.segmentMimeTypes.has(meta.mime.toLowerCase())) {
            throw new Error(`Unsupported segment MIME type: ${meta.mime}`);
        }
    }

    // Private methods

    private async _process(segment: HlsFetcherObject, controller: TransformStreamDefaultController): Promise<undefined> {

        // Check for new map entry

        if (!internals.isSameMap(segment.entry.map, this.#readState.map)) {
            this.#readState.map = segment.entry.map;

            // Fetch init segment

            if (segment.entry.map) {
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
                        fetch = await this._fetchFrom(this._tokenForMsn(segment.msn, segment.entry.map), { uri, byterange });
                    }
                    catch (err: any) {
                        if (tries >= 4) {
                            throw err;
                        }

                        //this.emit('problem', new Error('Failed to fetch map: ' + err.message));

                        // delay and retry

                        await new Promise((resolve) => setTimeout(resolve, 200 * (segment.entry.duration || 4)));
                        assert(!this.destroyed, 'destroyed');
                    }
                } while (!fetch);

                assert(!this.destroyed, 'destroyed');
                controller.enqueue(new HlsStreamerObject(fetch.meta, fetch.stream, 'map', segment.entry.map));

                // It is a fatal inconsistency error, if the map stream fails to download

                if (fetch.stream) {
                    fetch.stream.on('error', (err) => {

                        controller.error(new Error('Failed to download map data: ' + err.message));
                    });
                }
            }
        }

        // Fetch the segment

        await segment.closed();
        if (segment.entry.isPartial()) {
            return;
        }

        const fetch = await this._fetchFrom(this._tokenForMsn(segment.msn), { uri: segment.entry.uri!, byterange: segment.entry.byterange });
        assert(!this.destroyed, 'destroyed');

        // At this point object.stream has only been readied / opened

        let stream = fetch.stream;
        try {
            // Check meta

            if (this.reader && fetch.meta.modified) {
                const segmentTime = segment.entry.program_time || new Date(+fetch.meta.modified - (segment.entry.duration || 0) * 1000);

                if (!this.#started && this.reader.fetch.startDate &&
                    segmentTime < this.reader.fetch.startDate) {

                    return;   // Too early - ignore segment
                }

                if (this.reader.fetch.stopDate &&
                    segmentTime > this.reader.fetch.stopDate) {

                    controller.terminate();
                    return;
                }
            }

            // Track embedded stream to append more parts later

            if (stream) {
                this.#active.set(segment.msn, fetch);
                finished(stream, () => this.#active.delete(segment.msn));
            }

            controller.enqueue(new HlsStreamerObject(fetch.meta, stream, 'segment', segment));
            stream = undefined; // Don't destroy

            this.#started = true;
        }
        finally {
            stream?.destroy();
        }
    }

    private async _fetchFrom(token: number | string, part: { uri: string; byterange?: Required<Byterange> }) {

        const { uri, byterange } = part;
        const fetch = await this.#downloader.fetchSegment(token, new URL(uri, this.baseUrl), byterange);

        try {
            this.validateSegmentMeta(fetch.meta);

            return fetch;
        }
        catch (err) {
            if (fetch.stream && !fetch.stream.destroyed) {
                fetch.stream.destroy();
            }

            throw err;
        }
    }

    private _tokenForMsn(msn: number, map?: AttrList): number | string {

        if (map) {
            return map.toString();
        }

        return msn; // TODO: handle start over – add generation
    }

    indexUpdate(index: Readonly<MediaPlaylist>) {

        const old = this.#readState.indexTokens;

        const current = new Set<number | string>();
        for (let i = index.startMsn(true); i <= index.lastMsn(true); ++i) {
            const segment = index.getSegment(i);
            if (segment) {
                const token = this._tokenForMsn(i);
                current.add(token);
                old.delete(token);

                const map = segment.map;
                if (map) {
                    const mapToken = this._tokenForMsn(i, map);
                    current.add(mapToken);
                    old.delete(mapToken);
                }
            }
        }

        this.#readState.indexTokens = current;
        this.#readState.activeTokens = new Set([...old, ...current]);

        this.#downloader.setValid(this.#readState.activeTokens);
    }
}

export class HlsSegmentStreamer extends TransformStream {

    source: HlsSegmentDataSource;

    constructor(reader: HlsSegmentReadable, options: HlsSegmentStreamerOptions = {}) {

        assert(reader, 'A reader is required');

        let source;
        super(
            source = new HlsSegmentDataSource(reader, options),
            new CountQueuingStrategy({ highWaterMark: 1 }),
            new CountQueuingStrategy({ highWaterMark: options.highWaterMark ?? 0 })
        );

        this.source = source;

        reader.pipeThrough(this);

        this.handleIndexUpdate().catch(() => undefined);
    }

    protected async handleIndexUpdate(): Promise<void> {

        const { index } = await this.source.reader.fetch.source.next();

        if (index.master) {
            this.source.reader.cancel(new Error('The reader source is a master playlist'));
            return;
        }

        // Update active token list

        this.source.indexUpdate(index);

        // Wait for next update

        return this.handleIndexUpdate();
    }
}
