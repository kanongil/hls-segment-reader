import { createHash } from 'crypto';
import { Readable } from 'stream';

import { expect } from '@hapi/code';
import { AttrList, MediaSegment, IndependentSegment } from 'm3u8parse';

import { provisionServer, provisionLiveServer, genIndex, ServerState, UnprotectedPlaylistFetcher, expectCause, FakeFetcher } from './_shared.js';

// eslint-disable-next-line @hapi/capitalize-modules
import { createSimpleReader, HlsFetcherObject, HlsSegmentReadable, HlsSegmentStreamer, HlsStreamerObject } from '../lib/index.js';
import { HlsSegmentStreamerOptions } from '../lib/segment-streamer.js';
import { HlsPlaylistFetcher, HlsPlaylistFetcherOptions } from 'hls-playlist-reader/fetcher';
import { ContentFetcher, Deferred, wait } from 'hls-playlist-reader/helpers';
import { HlsSegmentFetcher, HlsSegmentFetcherOptions } from '../lib/segment-fetcher.js';


// Declare internals

const internals = {
    checksums: [
        'a6b0e0ce44f29e965e751113b39fdf4a47787cab',
        'c38d0718851a20be2edba13fc1643c1076826c62',
        '612991f34ae7cc19df5d595a2a4249b8f5d2d3f0',
        'bc600f4039aae412c4d978b3fd4d608ce4dec59a'
    ]
};


const nextValue = async function<T> (iter: AsyncIterator<T>, expectDone = false) {

    const { value, done } = await iter.next();

    expect(done).to.equal(expectDone);

    return value as T;
};

const devNull = async (stream?: ReadableStream | Readable): Promise<number> => {

    if (stream instanceof ReadableStream) {
        return new Promise<number>((resolve, reject) => {

            let consumed = 0;
            stream.pipeTo(new WritableStream<Uint8Array>({
                abort: reject,
                write(c) {

                    consumed += c.byteLength;
                },
                close: () => resolve(consumed)
            })).catch(reject);
        });
    }
    else if (stream instanceof Readable) {
        let consumed = 0;
        for await (const chunk of stream) {
            consumed += chunk.byteLength;
        }

        return consumed;
    }

    throw new Error('Missing or invalid stream');
};


describe('HlsSegmentStreamer()', () => {

    const contentFetcher = new ContentFetcher();

    const readSegments = function (url: string, options?: HlsSegmentStreamerOptions & HlsPlaylistFetcherOptions): Promise<HlsStreamerObject[]> {

        const r = new HlsSegmentStreamer(new HlsSegmentReadable(new HlsSegmentFetcher(new HlsPlaylistFetcher(url, contentFetcher, options), options)), options);
        const reader = r.getReader();
        const segments: HlsStreamerObject[] = [];

        return (async () => {

            for (; ;) {
                const { done, value } = await reader.read();
                if (done) {
                    return segments;
                }

                segments.push(value as any);
            }
        })();
    };

    let server: Awaited<ReturnType<typeof provisionServer>>;

    before(async () => {

        server = await provisionServer();
        return server.start();
    });

    after(() => {

        return server.stop();
    });

    describe('constructor', () => {

        it('creates a valid object', () => {

            const r = new HlsSegmentStreamer(new ReadableStream());

            expect(r).to.be.instanceOf(HlsSegmentStreamer);
        });
    });

    it('emits error for missing data', async () => {

        const promise = readSegments(`${server.info.uri}/notfound`);
        const err = await expect(promise).to.reject(Error);
        expectCause(err, 'Not Found');
    });

    it('emits error on unknown segment mime type', async () => {

        await expect((async () => {

            const r = createSimpleReader(`${server.info.uri}/simple/badtype.m3u8`, { withData: false });

            for await (const obj of r) {
                expect(obj).to.exist();
            }
        })()).to.reject(Error, /Unsupported segment MIME type/);

        await expect((async () => {

            const r = createSimpleReader(`${server.info.uri}/simple/badtype-data.m3u8`, { withData: true });

            for await (const obj of r) {
                expect(obj).to.exist();
            }
        })()).to.reject(Error, /Unsupported segment MIME type/);
    });

    describe('Using FakeFetcher manual ingestion', () => {

        const IMediaSegment = MediaSegment as MediaSegment & {
            new(obj?: unknown): IndependentSegment;
        };

        let ac: AbortController;
        let opts: { baseUrl: string; signal: AbortSignal };

        beforeEach(() => {

            ac = new AbortController();
            opts = { baseUrl: 'data:', signal: ac.signal };
        });

        afterEach(() => ac.abort());

        it('works', async () => {

            const fetcher = new FakeFetcher();
            const streamer = new HlsSegmentStreamer(new HlsSegmentReadable(fetcher));
            const iter = streamer[Symbol.asyncIterator]();

            fetcher.feed(new HlsFetcherObject(0, new IMediaSegment({
                uri: 'data:video/mp2t,TS',
                duration: 2
            }), opts));

            const obj = await nextValue(iter);
            expect(obj.type).to.equal('segment');
            expect(obj.segment!.msn).to.equal(0);

            fetcher.end();
            await nextValue(iter, true);
        });

        it('returns map objects', async () => {

            const fetcher = new FakeFetcher();
            const streamer = new HlsSegmentStreamer(new HlsSegmentReadable(fetcher));

            const segment = new IMediaSegment({
                uri: 'data:video/mp2t,DATA',
                duration: 2,
                map: new AttrList({ uri: '"data:video/mp2t,MAP"', value: 'OK' })
            });

            fetcher.feed(new HlsFetcherObject(0, segment, opts));
            fetcher.feed(new HlsFetcherObject(1, segment, opts));
            fetcher.end();

            const segments = [];
            for await (const obj of streamer) {
                segments.push(obj);
            }

            expect(segments).to.have.length(3);
            expect(segments[0].type).to.equal('map');
            expect(segments[0].attrs).to.equal(segment.map);
            expect(segments[1].type).to.equal('segment');
            expect(segments[1].segment!.msn).to.equal(0);
            expect(segments[2].type).to.equal('segment');
            expect(segments[2].segment!.msn).to.equal(1);
        });

        it('returns updated map objects', async () => {

            const fetcher = new FakeFetcher();
            const streamer = new HlsSegmentStreamer(new HlsSegmentReadable(fetcher));

            const segment = new IMediaSegment({
                uri: 'data:video/mp2t,DATA',
                duration: 2
            });

            fetcher.feed(new HlsFetcherObject(0, segment, opts));
            fetcher.feed(new HlsFetcherObject(1, new IMediaSegment({ ...segment, map: new AttrList({ uri: '"data:video/mp2t,MAP1"' }) }), opts));
            fetcher.feed(new HlsFetcherObject(2, new IMediaSegment({ ...segment, map: new AttrList({ uri: '"data:video/mp2t,MAP2"' }) }), opts));
            fetcher.feed(new HlsFetcherObject(3, new IMediaSegment({ ...segment, map: new AttrList({ uri: '"data:video/mp2t,MAP3"', byterange: '2@0' }) }), opts));
            fetcher.feed(new HlsFetcherObject(4, new IMediaSegment({ ...segment, map: new AttrList({ uri: '"data:video/mp2t,MAP3"', byterange: '3@1' }) }), opts));
            fetcher.feed(new HlsFetcherObject(5, segment, opts));
            fetcher.end();

            const segments = [];
            for await (const obj of streamer) {
                segments.push(obj);
            }

            expect(segments).to.have.length(10);

            expect(segments[0].type).to.equal('segment');
            expect(segments[0].segment!.msn).to.equal(0);
            expect(segments[1].type).to.equal('map');
            expect(segments[2].segment!.msn).to.equal(1);
            expect(segments[3].type).to.equal('map');
            expect(segments[4].segment!.msn).to.equal(2);
            expect(segments[5].type).to.equal('map');
            expect(segments[6].segment!.msn).to.equal(3);
            expect(segments[7].type).to.equal('map');
            expect(segments[8].segment!.msn).to.equal(4);
            expect(segments[9].segment!.msn).to.equal(5);
        });

        it('handles partial segments, where part completes', async () => {

            const fetcher = new FakeFetcher();
            const streamer = new HlsSegmentStreamer(new HlsSegmentReadable(fetcher));
            const iter = streamer[Symbol.asyncIterator]();

            const segment = new HlsFetcherObject(0, new IMediaSegment({
                parts: [new AttrList(), new AttrList()]
            }), opts);

            fetcher.feed(segment);

            const obj = await nextValue(iter);
            expect(obj.segment).to.equal(segment);

            // Finalize

            process.nextTick(() => {

                segment.entry = new IMediaSegment({
                    ...segment.entry,
                    uri: 'data:video/mp2t,TS',
                    duration: 2
                });
            });

            fetcher.feed(new HlsFetcherObject(1, new IMediaSegment({
                parts: [new AttrList(), new AttrList()]
            }), opts));

            const promise = nextValue(iter);
            expect(await Promise.race([segment.closed(), promise])).to.equal(true);

            expect(obj.segment).to.equal(segment); // Updated to match new entry
            expect((await promise).segment!.msn).to.equal(1);

            fetcher.end();
            await nextValue(iter, true);
        });

        it('handles partial segments with part hint', async () => {

            const fetcher = new FakeFetcher();
            const streamer = new HlsSegmentStreamer(new HlsSegmentReadable(fetcher));
            const iter = streamer[Symbol.asyncIterator]();

            const segment1 = new HlsFetcherObject(0, new IMediaSegment({
                parts: [new AttrList({ uri: '"data:video/mp2t,0.0"' }), new AttrList({ uri: '"data:video/mp2t,0.1"' })]
            }), opts);

            fetcher.feed(segment1);

            const segment2 = new HlsFetcherObject(1, new IMediaSegment({
                parts: undefined
            }), opts);
            segment2.hints = { part: { uri: 'data:video/mp2t,1.0' } };

            fetcher.feed(segment2);

            expect((await nextValue(iter)).segment).to.equal(segment1);
            const promise = nextValue(iter);

            await wait(1);

            segment2.entry = new IMediaSegment({
                parts: [new AttrList({ uri: '"data:video/mp2t,1.0"' })]
            });
            segment2.hints = { part: { uri: 'data:video/mp2t,1.1' } };

            fetcher.end();

            expect((await promise).segment).to.equal(segment2);
            await nextValue(iter, true);
        });
    });

    /*describe('master index', () => {

        it('does not output any segments', async () => {

            const reader = new HlsSegmentReadable(`${server.info.uri}/simple/index.m3u8`);
            await expect(readSegments(reader)).to.reject('Source cannot be based on a master playlist');
            expect((await reader.fetch.source.index()).index.master).to.be.true();
        });
    });*/

    describe('on-demand index', () => {

        it('outputs all segments', async () => {

            const segments = await readSegments(`${server.info.uri}/simple/500.m3u8`);

            expect(segments).to.have.length(3);
            for (let i = 0; i < segments.length; ++i) {
                expect(segments[i].segment!.msn).to.equal(i);
            }
        });

        it('handles byte-range', async () => {

            const r = createSimpleReader(`${server.info.uri}/simple/single.m3u8`, { withData: true });
            const checksums: string[] = [];

            for await (const obj of r) {
                const hasher = createHash('sha1');

                if (obj.stream instanceof ReadableStream) {
                    await obj.stream.pipeTo(new WritableStream({
                        write(chunk) {

                            hasher.update(chunk);
                        }
                    }));
                }
                else if (obj.stream instanceof Readable) {
                    for await (const chunk of obj.stream) {
                        hasher.update(chunk);
                    }
                }

                checksums.push(hasher.digest().toString('hex'));
            }

            expect(checksums).to.equal(internals.checksums);
        });

        it('does not internally buffer (highWaterMark=0)', async () => {

            const streamer = new HlsSegmentStreamer(new HlsSegmentReadable(new HlsSegmentFetcher(new HlsPlaylistFetcher(`${server.info.uri}/simple/long.m3u8`, contentFetcher))), { withData: false, highWaterMark: 0 });

            let reads = 0;
            for await (const obj of streamer) {
                ++reads;
                expect(obj).to.exist();
                expect(streamer.source.transforms - reads).to.equal(0);
                await wait(20);
                expect(streamer.source.transforms - reads).to.equal(0);
            }
        });

        it('supports the highWaterMark option', async () => {

            const r = createSimpleReader(`${server.info.uri}/simple/long.m3u8`, { highWaterMark: 3 });
            const buffered = [];

            let reads = 0;
            for await (const obj of r) {
                ++reads;
                expect(obj).to.exist();
                await wait(20);
                buffered.push(r.source.transforms - reads);
            }

            expect(buffered).to.equal([3, 3, 3, 2, 1, 0]);
        });

        it('cancel() also aborts active streams when withData is set', async () => {

            const r = createSimpleReader(`${server.info.uri}/simple/slow.m3u8`, { withData: true, highWaterMark: 2 });
            const segments: HlsStreamerObject[] = [];

            const reader = r.getReader();
            setTimeout(() => void reader.cancel(), 50);

            for (;;) {
                const { done, value } = await reader.read();
                if (done) {
                    break;
                }

                expect(value.segment!.msn).to.equal(segments.length);
                segments.push(value);
            }

            expect(segments.length).to.equal(2);

            const results = await Promise.allSettled(segments.map((segment) => devNull(segment.stream)));
            expect(results.map((res) => res.status)).to.equal(['fulfilled', 'rejected']);
        });

        /*it('abort() graceful is respected', async () => {

            const r = createSimpleReader(`${server.info.uri}/simple/slow.m3u8`, { withData: true, stopDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });
            const checksums = [];

            for await (const obj of r) {
                const hasher = Crypto.createHash('sha1');
                hasher.setEncoding('hex');

                obj.stream.pipe(hasher);
                const hash = await new Promise((resolve, reject) => {

                    obj.stream.on('error', reject);
                    obj.stream.on('end', () => resolve(hasher.read()));
                });

                checksums.push(hash);

                if (obj.segment.msn === 1) {
                    r.abort(true);
                }
            }

            expect(checksums).to.equal(internals.checksums.slice(1, 3));
        });*/

        it('can be cancelled', async () => {

            const readable = new HlsSegmentReadable(new HlsSegmentFetcher(new HlsPlaylistFetcher(`${server.info.uri}/simple/500.m3u8`, contentFetcher)));
            const r = new HlsSegmentStreamer(readable);

            let cancelled = false;
            const orig = readable.source.fetch.cancel;
            readable.source.fetch.cancel = (err) => {

                cancelled = true;
                return orig.call(readable.source.fetch, err);
            };

            const reader = r.getReader();
            await reader.read();
            await reader.cancel();

            await wait(0);
            expect(cancelled).to.be.true();
        });

        // handles all kinds of segment reference url
        // handles .m3u files
    });

    describe('live index', () => {

        const serverState = {} as { state: ServerState };
        let liveServer: Awaited<ReturnType<typeof provisionServer>>;

        const prepareLiveReader = function (readerOptions: HlsSegmentFetcherOptions & HlsSegmentStreamerOptions & HlsPlaylistFetcherOptions = {}, state: Partial<ServerState> = {}): {
            reader: HlsSegmentStreamer;
            state: ServerState & {
                error?: number;
                jumped?: boolean;
            };
        } {

            const fetcher = new HlsPlaylistFetcher(`${liveServer.info.uri}/live/live.m3u8`, contentFetcher, readerOptions);
            const streamer = new HlsSegmentStreamer(new HlsSegmentReadable(new HlsSegmentFetcher(fetcher, { fullStream: false, ...readerOptions })), { withData: true, ...readerOptions });

            const _fetcher = fetcher as any as UnprotectedPlaylistFetcher;
            const superFn = _fetcher.getUpdateInterval;
            _fetcher._intervals = [];
            _fetcher.getUpdateInterval = function (...args) {

                this._intervals.push(superFn.call(this, ...args));
                return undefined;
            };

            serverState.state = { firstMsn: 0, segmentCount: 10, targetDuration: 2, ...state };

            return { reader: streamer, state: serverState.state };
        };

        before(() => {

            liveServer = provisionLiveServer(serverState);
            return liveServer.start();
        });

        after(() => {

            return liveServer.stop();
        });

        it('handles a basic stream', async () => {

            const { reader, state } = prepareLiveReader({ fullStream: true });
            const segments = [];

            for await (const obj of reader) {
                expect(obj.segment!.msn).to.equal(segments.length);
                segments.push(obj);

                if (obj.segment!.msn > 5) {
                    state.firstMsn++;
                    if (state.firstMsn >= 5) {
                        state.firstMsn = 5;
                        state.ended = true;
                    }
                }
            }

            expect(segments.length).to.equal(15);
        });

        it('handles sequence number resets', async () => {

            let reset = false;
            const { reader, state } = prepareLiveReader({ fullStream: false }, { firstMsn: 10, async index() {

                const index = genIndex(state);

                if (!reset) {
                    state.firstMsn++;

                    if (state.firstMsn === 16) {
                        state.firstMsn = 0;
                        state.segmentCount = 1;
                        reset = true;
                    }

                    await wait(20);    // give the reader a chance to catch up
                }
                else {
                    state.segmentCount++;
                    if (state.segmentCount === 5) {
                        state.ended = true;
                    }
                }

                return index;
            } });

            const segments = [];
            for await (const obj of reader) {
                segments.push(obj);
            }

            expect(segments.length).to.equal(14);
            expect(segments[0].segment!.msn).to.equal(16);
            expect(segments[9].segment!.msn).to.equal(0);
            expect(segments[8].segment!.entry.discontinuity).to.be.false();
            expect(segments[9].segment!.entry.discontinuity).to.be.true();
            expect(segments[10].segment!.entry.discontinuity).to.be.false();
        });

        it('handles sequence number jumps', async () => {

            let skipped = false;
            const { reader, state } = prepareLiveReader({}, {
                index() {

                    const index = genIndex(state);

                    if (!skipped) {
                        ++state.firstMsn;
                        if (state.firstMsn === 5) {
                            state.firstMsn = 50;
                            skipped = true;
                        }
                    }
                    else if (skipped) {
                        ++state.firstMsn;
                        if (state.firstMsn === 55) {
                            state.ended = true;
                        }
                    }

                    return index;
                }
            });

            const segments = [];
            for await (const obj of reader) {
                segments.push(obj);
            }

            expect(segments.length).to.equal(20);
            expect(segments[4].segment!.msn).to.equal(10);
            expect(segments[4].segment!.entry.discontinuity).to.be.false();
            expect(segments[5].segment!.msn).to.equal(50);
            expect(segments[5].segment!.entry.discontinuity).to.be.true();
            expect(segments[6].segment!.entry.discontinuity).to.be.false();
        });

        // TODO: test problem emit & data outage
        /*it('handles a temporary server outage', async () => {

            const { reader, state } = prepareLiveReader({}, {
                index() {

                    if (state.error === undefined && state.firstMsn === 5) {
                        state.error = 6;
                    }

                    if (state.error) {
                        --state.error;
                        ++state.firstMsn;
                        throw new Error('fail');
                    }

                    if (state.firstMsn === 20) {
                        state.ended = true;
                    }

                    const index = Shared.genIndex(state);

                    ++state.firstMsn;

                    return index;
                }
            });

            const errors = [];
            reader.on('problem', errors.push.bind(errors));

            const segments = [];
            for await (const obj of reader) {
                expect(obj.msn).to.equal(segments.length);
                segments.push(obj);
            }

            expect(segments.length).to.equal(30);
            expect(errors.length).to.be.greaterThan(0);
            expect(errors[0]).to.be.an.error('Internal Server Error');
        });*/

        it('completes unstable downloads', async function () {

            // eslint-disable-next-line no-constant-condition
            if (true /** currently only works when uristream is used for fetching */) {
                return this.skip();
            }

            const { reader, state } = prepareLiveReader({}, { unstable: 1 });
            const segments = [];

            for await (const obj of reader) {
                expect(obj.segment!.msn).to.equal(segments.length + 6);
                segments.push(obj);

                let bytes = 0;
                for await (const chunk of obj.stream!) {
                    bytes += chunk.length;
                }

                expect(bytes).to.equal(5000 + obj.segment!.msn);

                if (obj.segment!.msn > 5) {
                    state.firstMsn++;
                    if (state.firstMsn >= 5) {
                        state.firstMsn = 5;
                        state.ended = true;
                    }
                }
            }

            expect(segments.length).to.equal(9);
        });

        it('aborts downloads that have been evicted from index', async () => {

            // Note: the eviction logic works on index updates, with a delay to allow an initial segment load some time to complete - otherwise it could be scheduled, have an immediate update, and be aborted before being given a chance

            const deliver = new Deferred<void>();

            const { reader, state } = prepareLiveReader({ fullStream: true }, { segmentCount: 3, slow: deliver.promise });
            const segments: HlsStreamerObject[] = [];

            const ac = new AbortController();
            state.signal = ac.signal;

            for await (const obj of reader) {
                segments.push(obj);

                state.firstMsn++;
                if (state.firstMsn >= 3) {
                    state.firstMsn = 3;
                    state.ended = true;
                }
            }

            expect(segments.length).to.equal(6);

            deliver.resolve();   // Signal server to finish the slow stream delivery

            // Empty all streams

            const results = await Promise.allSettled(segments.map((segment) => devNull(segment.stream)));
            expect(results.map((r) => r.status)).to.equal(['rejected', 'rejected', 'rejected', 'fulfilled', 'fulfilled', 'fulfilled']);
        });
    });
});
