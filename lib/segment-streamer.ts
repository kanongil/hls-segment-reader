
import type { Readable } from 'stream';
import type { MasterPlaylist, MediaPlaylist } from 'm3u8parse';
import type { HlsSegmentReader, HlsReaderObject } from './segment-reader';
import type { FetchResult, Byterange } from 'hls-playlist-reader/lib/helpers';

import { Stream, finished } from 'stream';
import { URL } from 'url';

import { assert as hoekAssert } from '@hapi/hoek';
import { AttrList } from 'm3u8parse';

import { DuplexEvents, TypedEmitter, TypedTransform } from 'hls-playlist-reader/lib/raw/typed-readable';
import { SegmentDownloader } from './segment-downloader';

import { types as MimeTypes } from 'mime-types';

/* eslint-disable @typescript-eslint/dot-notation */
MimeTypes['ac3'] = 'audio/ac3';
MimeTypes['eac3'] = 'audio/eac3';
MimeTypes['m4s'] = 'video/iso.segment';
/* eslint-enable @typescript-eslint/dot-notation */


// eslint-disable-next-line func-style
function assert(condition: any, ...args: any[]): asserts condition {

    hoekAssert(condition, ...args);
}

// eslint-disable-next-line func-style
function assertReaderObject(obj: any, message: string): asserts obj is HlsReaderObject {

    assert(typeof obj.msn === 'number' && obj.entry, message);
}


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

        return err.message === 'Aborted';
    }
};


export class HlsStreamerObject {

    type: 'segment' | 'map';
    file: FetchResult['meta'];
    stream?: Readable;
    segment?: HlsReaderObject;
    attrs?: AttrList;

    constructor(fileMeta: FetchResult['meta'], stream: Readable | undefined, type: 'map', details: AttrList);
    constructor(fileMeta: FetchResult['meta'], stream: Readable | undefined, type: 'segment', details: HlsReaderObject);

    constructor(fileMeta: FetchResult['meta'], stream: Readable | undefined, type: 'segment' | 'map', details: HlsReaderObject | AttrList) {

        const isSegment = type === 'segment';

        this.type = type;
        this.file = fileMeta;
        this.stream = stream;

        if (isSegment) {
            this.segment = details as HlsReaderObject;
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

const HlsSegmentStreamerEvents = <IHlsSegmentStreamerEvents & DuplexEvents<HlsStreamerObject>>(null as any);
interface IHlsSegmentStreamerEvents {
    problem(err: Error): void;
}

export class HlsSegmentStreamer extends TypedEmitter(HlsSegmentStreamerEvents, TypedTransform<HlsReaderObject, HlsStreamerObject>()) {

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
    #reader?: HlsSegmentReader;
    #started = false;

    #onReaderIndex = this._onReaderIndex.bind(this);
    #onReaderProblem = this._onReaderProblem.bind(this);

    constructor(reader?: HlsSegmentReader, options: HlsSegmentStreamerOptions = {}) {

        super({ objectMode: true, allowHalfOpen: false, autoDestroy: false, writableHighWaterMark: 0, readableHighWaterMark: (reader as any)?.highWaterMark ?? options.highWaterMark ?? 0 });

        // autoDestroy is broken for transform streams on node 14, so we need to manually emit 'close' after 'end'
        // Don't actually call destroy(), since it will trigger an abort() that aborts all tracked segment fetches

        this.on('end', () => process.nextTick(() => this.emit('close')));

        if (typeof reader === 'object' && !(reader instanceof Stream)) {
            options = reader;
            reader = undefined;
        }

        this.withData = options.withData ?? true;

        this.#downloader = new SegmentDownloader({ probe: !this.withData });

        this.on('pipe', (src: HlsSegmentReader) => {

            assert(!this.#reader, 'Only one piped source is supported');
            assert(!src.feeder.index?.master, 'Source cannot be based on a master playlist');

            this.#reader = src;
            src.on<'index'>('index', this.#onReaderIndex);
            src.on<'problem'>('problem', this.#onReaderProblem);

            if (src.index) {
                process.nextTick(this._onReaderIndex.bind(this, src.index, { url: src.feeder.baseUrl }));
            }

            this.baseUrl = src.feeder.baseUrl;
        });

        this.on('unpipe', () => {

            this.#reader?.off<'index'>('index', this.#onReaderIndex);
            this.#reader?.off<'problem'>('problem', this.#onReaderProblem);
            this.#reader = undefined;
        });

        // Pipe to self

        if (reader) {
            reader.on('error', (err) => {

                if (!this.destroyed) {
                    this.destroy(err);
                }
            }).pipe(this);
        }
    }

    abort(graceful = false): void {

        if (!graceful) {
            this.#downloader.setValid();
        }

        if (!this.readable) {
            return;
        }

        this.push(null);
    }

    _destroy(err: Error | null, cb: unknown): void {

        if (this.#reader && !this.#reader.destroyed) {
            this.#reader.destroy(err || undefined);
        }

        super._destroy(err, cb as any);

        this.abort(!!err);
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

    _transform(segment: HlsReaderObject | unknown, _: unknown, done: (err?: Error) => void): void {

        assertReaderObject(segment, 'Only segment-reader segments are supported');

        this._process(segment).then(() => done(), (err) => {

            done(internals.isAbortedError(err) ? undefined : err);
        });
    }

    // Private methods

    protected _onReaderIndex(index: Readonly<MediaPlaylist | MasterPlaylist>, { url }: { url: string }): void {

        this.baseUrl = url;

        if (index.master) {
            this.destroy(new Error('The reader source is a master playlist'));
            return;
        }

        // Update active token list

        this._updateTokens(index);
        this.#downloader.setValid(this.#readState.activeTokens);
    }

    protected _onReaderProblem(err: Error): void {

        this.emit<'problem'>('problem', err);
    }

    private async _process(segment: HlsReaderObject): Promise<undefined> {

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

                        this.emit('problem', new Error('Failed to fetch map: ' + err.message));

                        // delay and retry

                        await new Promise((resolve) => setTimeout(resolve, 200 * (segment.entry.duration || 4)));
                        assert(!this.destroyed, 'destroyed');
                    }
                } while (!fetch);

                assert(!this.destroyed, 'destroyed');
                this.push(new HlsStreamerObject(fetch.meta, fetch.stream, 'map', segment.entry.map));

                // It is a fatal inconsistency error, if the map stream fails to download

                if (fetch.stream) {
                    fetch.stream.on('error', (err) => {

                        this.destroy(new Error('Failed to download map data: ' + err.message));
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

            if (this.#reader && fetch.meta.modified) {
                const segmentTime = segment.entry.program_time || new Date(+fetch.meta.modified - (segment.entry.duration || 0) * 1000);

                if (!this.#started && this.#reader.startDate &&
                    segmentTime < this.#reader.startDate) {

                    return;   // Too early - ignore segment
                }

                if (this.#reader.stopDate &&
                    segmentTime > this.#reader.stopDate) {

                    this.push(null);
                    return;
                }
            }

            // Track embedded stream to append more parts later

            if (stream) {
                this.#active.set(segment.msn, fetch);
                finished(stream, () => this.#active.delete(segment.msn));
            }

            this.push(new HlsStreamerObject(fetch.meta, stream, 'segment', segment));
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

    private _updateTokens(index: Readonly<MediaPlaylist>) {

        const old = this.#readState.indexTokens;

        const current = new Set<number | string>();
        for (let i = index.startMsn(true); i <= index.lastMsn(true); ++i) {
            const token = this._tokenForMsn(i);
            current.add(token);
            old.delete(token);

            const map = index.getSegment(i)!.map;
            if (map) {
                const mapToken = this._tokenForMsn(i, map);
                current.add(mapToken);
                old.delete(mapToken);
            }
        }

        this.#readState.indexTokens = current;
        this.#readState.activeTokens = new Set([...old, ...current]);
    }
}
