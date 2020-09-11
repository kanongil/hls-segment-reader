
import type { HlsSegmentReader, HlsReaderObject } from './segment-reader';
import type { MediaPlaylist, M3U8IndependentSegment } from 'm3u8parse/lib/m3u8playlist';
import type { FetchResult, Byterange } from './helpers';

import { Stream, finished } from 'stream';

import { assert as hoekAssert } from '@hapi/hoek';
import { AttrList } from 'm3u8parse/lib/attrlist';
import { M3U8Playlist } from 'm3u8parse/lib/m3u8playlist';

import { TypedTransform, DuplexEvents } from './raw/typed-readable';
import { SegmentDownloader } from './segment-downloader';
import { HlsSegmentObject } from './segment-object';

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

// TODO: don't error on abort while fetching meta !!!!!


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

export type HlsSegmentStreamerOptions = {
    highWaterMark?: number;
    withData?: boolean; // default true
};

// FIXME: Need to handle index updates to track lifetime of segments

export class HlsSegmentStreamer extends TypedTransform<{ msn: number; entry: M3U8IndependentSegment }, HlsSegmentObject, DuplexEvents<HlsSegmentObject>> {

    withData: boolean;

    #readState = new (class ReadState {
        indexTokens = new Set<number>();
        activeTokens = new Set<number>();

        //mapSeq: -1,
        map?: AttrList;
        fetching: Promise<HlsSegmentObject> | null = null;
        //active: false;
        //discont: false;
    })();

    #active = new Map<number, FetchResult>(); // used to stop buffering on expired segments
    #downloader: SegmentDownloader;
    #reader?: HlsSegmentReader;
    #onIndexUpdate = this._onIndexUpdate.bind(this);

    constructor(reader?: HlsSegmentReader, options: HlsSegmentStreamerOptions = {}) {

        super({ objectMode: true, highWaterMark: (reader || {} as any).highWaterMark || options.highWaterMark || 0 });

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

    get index(): M3U8Playlist | undefined {

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

    protected _onIndexUpdate(index: M3U8Playlist): void {

        // Update active token list

        if (!index.master) {
            this._updateTokens(M3U8Playlist.castAsMedia(index));
            this.#downloader.setValid(this.#readState.activeTokens);
        }

        this.emit('index', index);
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

                // TODO: what should init token be to not be invalidated???
                // TODO: what about fetch errors? Remove from this.#readState.map??
                // FIXME: serialize processing while waiting for an init segment??

                const fetch = await this._fetchFrom(this._tokenForMsn(-1 - segment.msn), { uri, byterange });

                this.push(new HlsSegmentObject(fetch.meta, fetch.stream, 'init', segment.entry.map));
            }
        }

        // Fetch the segment

        if (!segment.isClosed) {
            await segment.closed;
        }

        if (segment.entry.isPartial()) {
            return;
        }

        const fetch = await this._fetchFrom(this._tokenForMsn(segment.msn), { uri: segment.entry.uri!, byterange: segment.entry.byterange });

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

            this.push(new HlsSegmentObject(fetch.meta, fetch.stream, 'segment', segment));
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

    private async _fetchFrom(token: number, part: { uri: string; byterange?: Required<Byterange> }) {

        const { uri, byterange } = part;
        const fetch = await this.#downloader.fetchSegment(token, uri, byterange);

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

    private _tokenForMsn(msn: number) {

        return msn; // TODO: handle start over
    }

    private _updateTokens(index: MediaPlaylist) {

        const old = this.#readState.indexTokens;

        const current = new Set<number>();
        for (let i = index.startSeqNo(true); i <= index.lastSeqNo(true); ++i) {
            const token = this._tokenForMsn(i);
            current.add(token);
            old.delete(token);
        }

        this.#readState.indexTokens = current;
        if (this.#readState.discont) {
            this.#readState.activeTokens = this.#readState.indexTokens;
        }
        else {
            // Keep expired tokens until next update

            this.#readState.activeTokens = new Set([...old, ...current]);
        }
    }
}
