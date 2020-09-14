
import type { HlsSegmentReader, HlsReaderObject, HlsIndexMeta } from './segment-reader';
import type { MasterPlaylist, MediaPlaylist } from 'm3u8parse';
import type { FetchResult, Byterange, ReadableStream } from './helpers';

import { Stream, finished } from 'stream';
import { URL } from 'url';

import { assert as hoekAssert } from '@hapi/hoek';
import { AttrList } from 'm3u8parse';

import { TypedTransform, DuplexEvents } from './raw/typed-readable';
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
    stream?: ReadableStream;
    segment?: HlsReaderObject;
    attrs?: AttrList;

    constructor(fileMeta: FetchResult['meta'], stream: ReadableStream | undefined, type: 'map', details: AttrList);
    constructor(fileMeta: FetchResult['meta'], stream: ReadableStream | undefined, type: 'segment', details: HlsReaderObject);

    constructor(fileMeta: FetchResult['meta'], stream: ReadableStream | undefined, type: 'segment' | 'map', details: HlsReaderObject | AttrList) {

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

interface HlsSegmentStreamerEvents extends DuplexEvents<HlsStreamerObject> {
    index: (index: MediaPlaylist | MasterPlaylist, meta: HlsIndexMeta) => void;
    problem: (err: Error) => void;
}

// FIXME: Need to handle index updates to track lifetime of segments

export class HlsSegmentStreamer extends TypedTransform<HlsReaderObject, HlsStreamerObject, HlsSegmentStreamerEvents> {

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
    #onIndexUpdate = this._onIndexUpdate.bind(this);

    constructor(reader?: HlsSegmentReader, options: HlsSegmentStreamerOptions = {}) {

        super({ objectMode: true, writableHighWaterMark: 0, readableHighWaterMark: (reader as any)?.highWaterMark ?? options.highWaterMark ?? 0 });

        if (typeof reader === 'object' && !(reader instanceof Stream)) {
            options = reader;
            reader = undefined;
        }

        this.withData = options.withData ?? true;

        this.#downloader = new SegmentDownloader({ probe: !this.withData });

        this.on('pipe', (src: HlsSegmentReader) => {

            assert(!this.#reader, 'Only one piped source is supported');

            this.#reader = src;
            src.on('index', this.#onIndexUpdate);
        });

        this.on('unpipe', () => {

            this.#reader!.off('index', this.#onIndexUpdate);
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

        if (!this.readableEnded) {
            this.push(null);
        }
    }

    _destroy(err: Error | null, cb: unknown): void {

        // FIXME: is reader unpiped first???
        if (this.#reader && !this.#reader.destroyed) {
            this.#reader.off('index', this.#onIndexUpdate);
            this.#reader.destroy(err || undefined);
        }

        super._destroy(err, cb as any);

        this.abort(!!err);
    }

    get index(): MediaPlaylist | MasterPlaylist | undefined {

        return this.#reader ? this.#reader.index : undefined;
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

    protected _onIndexUpdate(index: MediaPlaylist | MasterPlaylist, meta: HlsIndexMeta): void {

        this.baseUrl = meta.url;

        // Update active token list

        if (!index.master) {
            this._updateTokens(index);
            this.#downloader.setValid(this.#readState.activeTokens);
        }

        this.emit('index', index, meta);
    }

    private async _process(segment: HlsReaderObject): Promise<undefined> {

        // Check for new map entry

        if (!internals.isSameMap(segment.entry.map, this.#readState.map)) {
            this.#readState.map = segment.entry.map;

            // Fetch init segment

            if (segment.entry.map) {
                const uri = segment.entry.map.get('uri', AttrList.Types.String);
                assert(uri, 'EXT-X-MAP must have URI attribute');
                let byterange;
                if (segment.entry.map.has('byterange')) {
                    // Byterange in map is _not_ byterange encoded - rather it is a quoted string!

                    const [length, offset = '0'] = segment.entry.map.get('byterange', AttrList.Types.String)!.split('@');
                    byterange = {
                        offset: parseInt(offset, 10),
                        length: parseInt(length, 10)
                    };
                }

                // Fetching the map is essential to the processing

                let fetch: FetchResult | undefined;
                let tries = 0;
                do {
                    try {
                        tries++;
                        fetch = await this._fetchFrom(this._tokenForMsn(segment.msn, segment.entry.map), { uri, byterange });
                    }
                    catch (err) {
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
                    fetch.stream.once('error', (err) => {

                        if (!this.destroyed) {
                            this.destroy(new Error('Failed to download map data: ' + err.message));
                        }
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

        try {
            // Check meta

            if (this.#reader && fetch.meta.modified) {
                const segmentTime = segment.entry.program_time || new Date(+fetch.meta.modified - (segment.entry.duration || 0) * 1000);

                if (this.#reader.startDate && segmentTime < this.#reader.startDate) {
                    throw new Error('too early');
                }

                if (this.#reader.stopDate && segmentTime > this.#reader.stopDate) {
                    // eslint-disable-next-line @typescript-eslint/no-throw-literal
                    throw 'ended';
                }
            }

            // Track embedded stream to append more parts later

            if (fetch.stream) {
                this.#active.set(segment.msn, fetch);
                finished(fetch.stream, () => this.#active.delete(segment.msn));
            }

            this.push(new HlsStreamerObject(fetch.meta, fetch.stream, 'segment', segment));
        }
        catch (err) {
            if (fetch.stream && !fetch.stream.destroyed) {
                fetch.stream.destroy(err);
            }

            if (err === 'ended') {
                this.push(null);
                return;
            }

            throw err;
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

    private _updateTokens(index: MediaPlaylist) {

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
