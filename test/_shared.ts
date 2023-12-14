import Fs from 'fs';
import { Readable } from 'stream';

import { expect } from '@hapi/code';
import Hapi from '@hapi/hapi';
import Inert from '@hapi/inert';
import Joi from 'joi';
import { AttrList, M3U8Playlist, MediaPlaylist, MediaSegment } from 'm3u8parse';
import { HlsPlaylistFetcher } from 'hls-playlist-reader/fetcher';
import { HlsFetcherObject, HlsSegmentFetcher } from '../lib/segment-fetcher.js';
import { ContentFetcher, Deferred, wait } from 'hls-playlist-reader/helpers';

const ignore = () => undefined;

export interface UnprotectedPlaylistFetcher {
    _intervals: (number | undefined)[];
    getUpdateInterval(...args: any[]): ReturnType<HlsPlaylistFetcher<any>['getUpdateInterval']>;
}

export const provisionServer = async () => {

    const server = new Hapi.Server({
        host: '127.0.0.1',
        routes: { files: { relativeTo: new URL('fixtures', import.meta.url).pathname } }
    });

    await server.register(Inert);

    const delay: Hapi.Lifecycle.Method = async (_request, _h) => {

        await wait(200);

        return 200;
    };

    const slowServe: Hapi.Lifecycle.Method = (request, h) => {

        const slowStream = new Readable();
        slowStream._read = ignore;

        const url = new URL(`fixtures/${request.params.path}`, import.meta.url);
        const buffer = Fs.readFileSync(url);
        slowStream.push(buffer.slice(0, 5000));
        setTimeout(() => {

            slowStream.push(buffer.slice(5000));
            slowStream.push(null);
        }, 250);

        return h.response(slowStream).type('video/mp2t').header('content-length', buffer.byteLength.toString());
    };

    server.route({ method: 'GET', path: '/simple/{path*}', handler: { directory: { path: '.' } } });
    server.route({ method: 'GET', path: '/slow/{path*}', handler: { directory: { path: '.' } }, options: { pre: [{ method: delay, assign: 'delay' }] } });
    server.route({ method: 'GET', path: '/slow-data/{path*}', handler: slowServe });
    server.route({
        method: 'GET', path: '/error', handler(request, h) {

            throw new Error('!!!');
        }
    });

    server.ext('onRequest', (request, h) => {

        if ((server as any).onRequest) {
            (server as any).onRequest(request);
        }

        return h.continue;
    });

    return server;
};

export interface ServerState extends IndexState {
    index?: (query: Hapi.RequestQuery) => Promise<M3U8Playlist | string> | M3U8Playlist | string;
    slow?: Promise<void>;
    unstable?: number;
    signal?: AbortSignal;
}

export const provisionLiveServer = function (shared: { state: ServerState }) {

    const server = new Hapi.Server({
        host: '127.0.0.1',
        routes: {
            files: { relativeTo: new URL('fixtures', import.meta.url).pathname }
        }
    });

    const serveLiveIndex: Hapi.Lifecycle.Method = async (request, h) => {

        let index: M3U8Playlist | string;
        if (shared.state.index) {
            index = await shared.state.index(request.query);
        }
        else {
            index = genIndex(shared.state);
        }

        return h.response(index.toString()).type('application/vnd.apple.mpegURL');
    };

    const serveSegment: Hapi.Lifecycle.Method = (request, h) => {

        if (shared.state.slow) {
            const slowStream = new Readable({ read: ignore });

            slowStream.push(Buffer.alloc(5000));

            void shared.state.slow.then(() => {

                slowStream.push(Buffer.alloc(2000));
                slowStream.push(null);
            });

            return h.response(slowStream).type('video/mp2t').bytes(7000);
        }

        const size = ~~(5000 / (request.params.part === undefined ? 1 : shared.state.partCount!)) + parseInt(request.params.msn) + 100 * parseInt(request.params.part || 0);

        if (shared.state.unstable) {
            --shared.state.unstable;

            const unstableStream = new Readable({ read: ignore });

            unstableStream.push(Buffer.alloc(50 - shared.state.unstable));
            unstableStream.push(null);

            unstableStream.once('end', () => {

                // Manually destroy socket in case it is a keep-alive connection
                // otherwise the receiver will never know that the request is done

                request.raw.req.destroy();
            });

            return h.response(unstableStream).type('video/mp2t').bytes(size);
        }

        return h.response(Buffer.alloc(size)).type('video/mp2t').bytes(size);
    };

    server.route({
        method: 'GET',
        path: '/live/live.m3u8',
        handler: serveLiveIndex,
        options: {
            validate: {
                query: Joi.object({
                    '_HLS_msn': Joi.number().integer().min(0).optional(),
                    '_HLS_part': Joi.number().min(0).optional()
                }).with('_HLS_part', '_HLS_msn')
            }
        }
    });
    server.route({ method: 'GET', path: '/live/{msn}.ts', handler: serveSegment });
    server.route({ method: 'GET', path: '/live/{msn}-part{part}.ts', handler: serveSegment });

    return server;
};


interface IndexState {
    targetDuration: number;
    segmentCount: number;
    firstMsn: number;
    partCount?: number;
    partIndex?: number;
    ended?: boolean;
    genCount?: number;
}

export const genIndex = function ({ targetDuration, segmentCount, firstMsn, partCount, partIndex, ended }: IndexState) {

    const partDuration = targetDuration / partCount!;

    const segments: MediaSegment[] = [];
    const meta: typeof MediaPlaylist.prototype['meta'] = {};

    for (let i = 0; i < segmentCount; ++i) {
        const parts = [];
        if (i >= segmentCount - 2) {
            for (let j = 0; j < partCount!; ++j) {
                parts.push(new AttrList({
                    duration: partDuration.toString(),
                    uri: `"${firstMsn + i}-part${j}.ts"`
                }));
            }
        }

        segments.push({
            duration: targetDuration || 2,
            uri: `${firstMsn + i}.ts`,
            title: '',
            parts: parts.length ? parts : undefined
        } as MediaSegment);
    }

    if (partIndex !== undefined) {
        if (partIndex > 0) {
            const parts = [];
            for (let i = 0; i < partIndex; ++i) {
                parts.push(new AttrList({
                    duration: partDuration.toString(),
                    uri: `"${firstMsn + segmentCount}-part${i}.ts"`
                }));
            }

            segments.push({ parts } as MediaSegment);
        }

        // Add hint

        if (!ended) {
            meta.preload_hints = [new AttrList({
                type: 'part',
                uri: `"${firstMsn + segmentCount}-part${partIndex}.ts"`
            })];
        }
    }

    const index = new MediaPlaylist({
        media_sequence: firstMsn,
        target_duration: targetDuration,
        part_info: partCount ? new AttrList({ 'part-target': partDuration.toString() }) : undefined,
        segments,
        meta,
        ended
    } as MediaPlaylist);

    //console.log('GEN', index.startMsn(true), index.lastMsn(true), index.meta.preload_hints, index.ended);

    return index;
};


export interface LlIndexState extends IndexState {
    end?: { msn: number; part?: number };
}

export const genLlIndex = function (query: Hapi.RequestQuery, state: LlIndexState) {

    // Return playlist with exactly the next part

    if (!state.ended && query._HLS_msn !== undefined) {
        let msn = query._HLS_msn;
        let part = query._HLS_part === undefined ? state.partCount : query._HLS_part + 1;

        if (part >= state.partCount!) {
            msn++;
            part = 0;
        }

        state.firstMsn = msn - state.segmentCount;
        state.partIndex = part;
    }

    const index = genIndex(state);

    index.server_control = new AttrList({
        'can-block-reload': 'YES',
        'part-hold-back': (3 * state.targetDuration / state.partCount!).toString()
    });

    state.genCount = (state.genCount || 0) + 1;

    if (!state.ended) {
        if (state.end &&
            (index.lastMsn() > state.end.msn || (index.lastMsn() === state.end.msn && state.end.part === index.getSegment(index.lastMsn())!.parts?.length))) {

            index.ended = state.ended = true;
            delete index.meta.preload_hints;
            return index;
        }

        state.partIndex = ~~state.partIndex! + 1;
        if (state.partIndex >= state.partCount!) {
            state.partIndex = 0;
            state.firstMsn++;
        }
    }

    return index;
};

export const expectCause = (err: any, match: string | RegExp): void => {

    expect(err).to.be.an.error();
    if (err.cause) {
        if (typeof match === 'string') {
            expect(err.cause).to.equal(match);
        }
        else {
            expect(err.cause).to.match(match);
        }
    }
    else {
        if (typeof match === 'string') {
            expect(err.message).to.equal(match);
        }
        else {
            expect(err.message).to.match(match);
        }
    }
};

export class FakeFetcher extends HlsSegmentFetcher {

    static readonly contentFetcher = new ContentFetcher();

    readonly queue: HlsFetcherObject[] = [];
    ended = false;

    #update = new Deferred<void>();

    constructor() {

        super(new HlsPlaylistFetcher('data:', FakeFetcher.contentFetcher));
    }

    feed(obj: HlsFetcherObject) {

        this.queue.push(obj);

        this.#update.resolve();
        this.#update = new Deferred<void>();
    }

    end() {

        this.ended = true;
        this.#update.resolve();
    }

    // Overriden implementation

    start() {

        return Promise.resolve(new MediaPlaylist());
    }

    async next(): Promise<HlsFetcherObject | null> {

        const obj = this.queue.shift();
        if (obj || this.ended) {
            return obj ?? null;
        }

        await this.#update.promise;

        return this.next();
    }

    cancel(reason?: Error | undefined): void {

        this.#update.reject(reason || new Error('aborted'));
    }
}
