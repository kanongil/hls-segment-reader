//import { promises } from 'fs';
//import { tmpdir } from 'os';
//import { join/*, sep*/ } from 'path';
//import { pathToFileURL } from 'url';

import { notFound, serverUnavailable, unauthorized } from '@hapi/boom';
import { expect } from '@hapi/code';
import { HlsPlaylistFetcher, HlsPlaylistFetcherOptions } from 'hls-playlist-reader/fetcher';
import { ContentFetcher, Deferred } from 'hls-playlist-reader/helpers';
import { wait } from '@hapi/hoek';
import { AttrList, MainPlaylist, MediaPlaylist } from 'm3u8parse';

import { expectCause, provisionServer, provisionLiveServer, genIndex, ServerState, UnprotectedPlaylistFetcher, LlIndexState, genLlIndex } from './_shared.js';
import { HlsSegmentReadable, HlsFetcherObject } from '../lib/index.js';
import { HlsSegmentFetcher, HlsSegmentFetcherOptions } from '../lib/segment-fetcher.js';


declare global {
    // Add AsyncIterator which is implemented by node.js
    interface ReadableStream<R = any> {
        [Symbol.asyncIterator](): AsyncIterator<R>;
    }
}


describe('HlsSegmentReadable()', () => {

    const contentFetcher = new ContentFetcher();

    const createReadable = (url: URL | string, options?: HlsSegmentFetcherOptions & HlsPlaylistFetcherOptions) => {

        const fetcher = new HlsSegmentFetcher(new HlsPlaylistFetcher(url, contentFetcher, options), options);
        return new HlsSegmentReadable(fetcher);
    };

    const readSegments = function (url: string, options?: HlsSegmentFetcherOptions & HlsPlaylistFetcherOptions): Promise<HlsFetcherObject[]> {

        const r = createReadable(url, options);
        const reader = r.getReader();
        const segments: HlsFetcherObject[] = [];

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

    beforeEach(() => {

        (server as any).onRequest = null;
    });

    after(() => {

        return server.stop();
    });

    describe('constructor', () => {

        it('creates a valid object', async () => {

            const r = new HlsSegmentReadable(new HlsSegmentFetcher(new HlsPlaylistFetcher(`${server.info.uri}/simple/500.m3u8`, contentFetcher)));

            expect(r).to.be.instanceOf(HlsSegmentReadable);

            await wait(10);
            await r.cancel();
        });

        it('throws on missing fetcher option', () => {

            const createObject = () => {

                return new (HlsSegmentReadable as any)();
            };

            expect(createObject).to.throw();
        });

        it('rejects on read() with invalid fetcher uri', async () => {

            const r = new HlsSegmentReadable(new HlsSegmentFetcher(new HlsPlaylistFetcher('asdf://test', contentFetcher)));
            const reader = r.getReader();

            await expect(reader.read()).to.reject();
            expect(r.locked).to.be.true();
        });
    });

    describe('master index', () => {

        it('does not output any segments', async () => {

            const segments = await readSegments(`${server.info.uri}/simple/index.m3u8`);
            expect(segments).to.have.length(0);
        });

        it('calls "onIndex" hook', async () => {

            let remoteIndex: Readonly<MainPlaylist> | undefined;
            const segments = await readSegments(`${server.info.uri}/simple/index.m3u8`, {
                onIndex(index) {

                    remoteIndex = index as MainPlaylist;
                }
            });

            expect(segments).to.have.length(0);
            expect(remoteIndex).to.exist();
            expect(remoteIndex!.master).to.be.true();
            expect(remoteIndex!.variants[0].uri).to.exist();
        });
    });

    describe('on-demand index', () => {

        it('outputs all segments', async () => {

            const segments = await readSegments(`${server.info.uri}/simple/500.m3u8`);

            expect(segments.length).to.equal(3);
            for (let i = 0; i < segments.length; ++i) {
                expect(segments[i].msn).to.equal(i);
            }
        });

        it('emits the "index" event before first read returns', async () => {

            const deferred = new Deferred();
            const readable = createReadable(`${server.info.uri}/simple/500.m3u8`, {
                onIndex() {

                    deferred.resolve('index');
                }
            });

            const reader = readable.getReader();
            const winner = await Promise.race([reader.read(), deferred.promise]);

            expect(winner).to.be.equal('index');
        });

        it('supports the startDate option', async () => {

            const r = createReadable(`${server.info.uri}/simple/500.m3u8`, { startDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });
            const segments = [];

            for await (const segment of r) {
                expect(segment.msn).to.equal(segments.length + 2);
                segments.push(segment);
            }

            expect(segments).to.have.length(1);
        });

        /*it('supports the stopDate option', async () => {

            const r = createReadable(`${server.info.uri}/simple/500.m3u8`, { stopDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });
            const segments = [];

            for await (const segment of r) {
                expect(segment.msn).to.equal(segments.length);
                segments.push(segment);
            }

            expect(segments).to.have.length(2);
        });*/

        it('applies the extensions option', async () => {

            const extensions = {
                '#EXT-MY-HEADER': false,
                '#EXT-MY-SEGMENT-OK': true
            };

            let index: Readonly<MediaPlaylist> | undefined;
            const r = createReadable(`${server.info.uri}/simple/500.m3u8`, { extensions,
                onIndex(newIndex) {

                    index = newIndex as MediaPlaylist;
                }
            });

            const segments = [];
            for await (const segment of r) {
                segments.push(segment);
            }

            expect(index).to.exist();
            expect(index!.vendor).to.equal([['#EXT-MY-HEADER', 'hello']]);
            expect(index!.segments[1].vendor).to.equal([['#EXT-MY-SEGMENT-OK', null]]);
            expect(segments).to.have.length(3);
            expect(segments[1].entry.vendor).to.equal([['#EXT-MY-SEGMENT-OK', null]]);
        });

        it('does not internally buffer', async () => {

            let nextCalls = 0;
            const r = createReadable(`${server.info.uri}/simple/long.m3u8`);
            const orig = r.source.fetch.next;
            r.source.fetch.next = (opts) => {

                ++nextCalls;
                return orig.call(r.source.fetch, opts);
            };

            let count = 0;
            for await (const obj of r) {
                ++count;
                expect(obj).to.exist();
                expect(nextCalls - count).to.equal(0);
                await wait(1);
                expect(nextCalls - count).to.equal(0);
            }
        });

        /*it('supports the highWaterMark option', async () => {

            const r = createReadable(`${server.info.uri}/simple/long.m3u8`, { highWaterMark: 2 });
            const buffered = [];

            for await (const obj of r) {
                expect(obj).to.exist();
                await Hoek.wait(20);
                buffered.push(r._readableState.buffer.length);
            }

            expect(buffered).to.equal([2, 2, 2, 2, 1, 0]);
        });*/

        it('can be cancelled', async () => {

            const r = createReadable(`${server.info.uri}/simple/500.m3u8`);

            let cancelled = false;
            const orig = r.source.fetch.cancel;
            r.source.fetch.cancel = (err) => {

                cancelled = true;
                return orig.call(r.source.fetch, err);
            };

            const segments = [];
            for await (const obj of r) {
                segments.push(obj);
                expect(r.locked).to.be.true();
                break;               // Iterator break cancels readable
            }

            expect(segments).to.have.length(1);
            expect(r.locked).to.be.false();
            expect(cancelled).to.be.true();
        });

        // handles all kinds of segment reference url
        // handles .m3u files
    });

    describe('live index', () => {

        const serverState = {} as { state: ServerState };
        let liveServer: Awaited<ReturnType<typeof provisionServer>>;

        const prepareLiveReader = function (readerOptions: HlsSegmentFetcherOptions & HlsPlaylistFetcherOptions = {}, state: Partial<ServerState> = {}): {
            reader: HlsSegmentReadable;
            state: ServerState & {
                error?: number;
                jumped?: boolean;
            };
        } {

            const reader = createReadable(`${liveServer.info.uri}/live/live.m3u8`, { fullStream: true, ...readerOptions });
            const fetch = reader.source.fetch.source as any as UnprotectedPlaylistFetcher;
            const superFn = fetch.getUpdateInterval;
            fetch._intervals = [];
            fetch.getUpdateInterval = function (...args) {

                this._intervals.push(superFn.call(this, ...args));
                return undefined;
            };

            serverState.state = { firstMsn: 0, segmentCount: 10, targetDuration: 2, ...state };

            return { reader, state: serverState.state };
        };

        before(() => {

            liveServer = provisionLiveServer(serverState);
            return liveServer.start();
        });

        after(() => {

            return liveServer.stop();
        });

        it('handles a basic stream (http)', async () => {

            const { reader, state } = prepareLiveReader();
            const segments = [];

            for await (const obj of reader) {
                expect(obj.msn).to.equal(segments.length);
                segments.push(obj);

                if (obj.msn > 5) {
                    state.firstMsn++;
                    if (state.firstMsn >= 5) {
                        state.firstMsn = 5;
                        state.ended = true;
                    }
                }
            }

            expect(segments).to.have.length(15);
        });

        /*it('handles a basic stream (file)', async () => {

            const state = serverState.state = { firstMsn: 0, segmentCount: 10, targetDuration: 10 };

            const tmpDir = await promises.mkdtemp(await promises.realpath(tmpdir()) + sep);
            try {
                const tmpUrl = new URL('next.m3u8', pathToFileURL(tmpDir + sep));
                const indexUrl = new URL('index.m3u8', pathToFileURL(tmpDir + sep));
                await promises.writeFile(indexUrl, genIndex(state).toString(), 'utf-8');

                const reader = createReadable(indexUrl.href, { fullStream: true });
                const segments = [];

                (async () => {

                    while (!state.ended) {
                        await wait(50);

                        state.firstMsn++;
                        if (state.firstMsn === 5) {
                            state.ended = true;
                        }

                        // Atomic write

                        await promises.writeFile(tmpUrl, genIndex(state).toString(), 'utf-8');
                        await promises.rename(tmpUrl, indexUrl);
                    }
                })();

                for await (const obj of reader) {
                    expect(obj.msn).to.equal(segments.length);
                    segments.push(obj);
                }

                expect(segments).to.have.length(15);
            }
            finally {
                await promises.rm(tmpDir, { recursive: true });
            }
        });*/

        it('can start with 0 segments', async () => {

            const { reader, state } = prepareLiveReader({}, { segmentCount: 0, index() {

                const index = genIndex(state);
                index.type = 'EVENT';

                if (state.segmentCount === 5) {
                    state.ended = true;
                }
                else {
                    state.segmentCount++;
                }

                return index;
            } });
            const segments = [];

            for await (const obj of reader) {
                expect(obj.msn).to.equal(segments.length);
                segments.push(obj);
            }

            expect(segments).to.have.length(5);
        });

        it('closes when cancelled without consuming', async () => {

            const { reader } = prepareLiveReader();

            const playlist = await (reader.source.fetch as any)._requestPlaylistUpdate();
            expect(playlist).to.exist();

            const r = reader.getReader();
            r.cancel();
            await r.closed;

            expect(reader.locked).to.be.true();
        });

        it('handles sequence number resets', async () => {

            let reset = false;
            const { reader, state } = prepareLiveReader({}, {
                firstMsn: 9,
                segmentCount: 5,
                index() {

                    if (!state.ended) {
                        if (!reset) {
                            state.firstMsn++;

                            if (state.firstMsn === 13) {
                                state.firstMsn = 0;
                                state.segmentCount = 1;
                                reset = true;
                            }
                        }
                        else {
                            state.segmentCount++;
                            if (state.segmentCount === 5) {
                                state.ended = true;
                            }
                        }
                    }

                    return genIndex(state);
                }
            });

            const segments = [];
            for await (const obj of reader) {
                segments.push(obj);
            }

            expect(segments).to.have.length(12);
            expect(segments[7].msn).to.equal(0);
            expect(segments[6].entry.discontinuity).to.be.false();
            expect(segments[7].entry.discontinuity).to.be.true();
            expect(segments[8].entry.discontinuity).to.be.false();
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

            expect(segments).to.have.length(29);
            expect(segments[13].msn).to.equal(13);
            expect(segments[13].entry.discontinuity).to.be.false();
            expect(segments[14].msn).to.equal(50);
            expect(segments[14].entry.discontinuity).to.be.true();
            expect(segments[15].entry.discontinuity).to.be.false();
        });

        it('handles a temporary server outage', async () => {

            const problems: Error[] = [];
            const { reader, state } = prepareLiveReader({ onProblem: problems.push.bind(problems) }, {
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

                    const index = genIndex(state);

                    ++state.firstMsn;

                    return index;
                }
            });

            const segments = [];
            for await (const obj of reader) {
                expect(obj.msn).to.equal(segments.length);
                segments.push(obj);
            }

            expect(segments).to.have.length(30);
            expect(problems.length).to.be.greaterThan(0);
            expectCause(problems[0], 'Internal Server Error');
        });

        it('drops segments when reader is slow', async () => {

            const { reader, state } = prepareLiveReader({ fullStream: false }, {
                index() {

                    if (state.firstMsn === 50) {
                        state.ended = true;
                    }

                    const index = genIndex(state);

                    return index;
                }
            });

            const segments = [];
            for await (const obj of reader) {
                segments.push(obj);

                state.firstMsn += 5;

                await wait(5);
            }

            expect(segments).to.have.length(20);
            expect(segments.map((s) => s.msn)).to.equal([
                6, 7, 10, 15, 20, 25, 30,
                35, 40, 45, 50, 51, 52, 53,
                54, 55, 56, 57, 58, 59
            ]);
            expect(segments.map((s) => s.entry.discontinuity)).to.equal([
                false, false, true, true, true, true, true,
                true, true, true, true, false, false, false,
                false, false, false, false, false, false
            ]);
        });

        it('respects the maxStallTime option', async () => {

            const { reader } = prepareLiveReader({ maxStallTime: 50 }, { segmentCount: 1 });

            await expect((async () => {

                for await (const obj of reader) {

                    expect(obj).to.exist();
                }
            })()).to.reject(Error, /Index update stalled/);
        });

        describe('cancel()', () => {

            it('works when called while waiting for a segment', async () => {

                const { reader, state } = prepareLiveReader({ fullStream: false }, {
                    async index() {

                        if (state.firstMsn > 0) {
                            await wait(100);
                        }

                        return genIndex(state);
                    }
                });

                const r = reader.getReader();
                setTimeout(() => r.cancel(), 50);

                const segments = [];
                for (;;) {
                    const { value, done } = await r.read();
                    if (done) {
                        break;
                    }

                    segments.push(value);
                    state.firstMsn++;
                }

                await r.closed;

                expect(segments).to.have.length(4);
            });

            it('forwards passed error', async () => {

                const { reader, state } = prepareLiveReader({ fullStream: false }, {
                    async index() {

                        if (state.firstMsn > 0) {
                            await wait(10);
                        }

                        return genIndex(state);
                    }
                });

                let sourceReason: Error | undefined;
                const orig = reader.source.fetch.source.cancel;
                reader.source.fetch.source.cancel = (reason) => {

                    sourceReason = reason;
                    return orig.call(reader.source.fetch.source, reason);
                };

                const r = reader.getReader();
                setTimeout(() => r.cancel(new Error('destroyed')), 50);

                for (; ;) {
                    const { done } = await r.read();
                    if (done) {
                        break;
                    }

                    state.firstMsn++;
                }

                await r.closed;

                expect(sourceReason?.message).to.equal('destroyed');
            });
        });

        // TODO: move
        describe('isRecoverableUpdateError()', () => {

            it('is called on index update errors', async () => {

                const { reader, state } = prepareLiveReader({}, {
                    index() {

                        const { error } = state;
                        if (error) {
                            state.error!++;
                            switch (error) {
                                case 1:
                                    throw notFound();
                                case 2:
                                    throw serverUnavailable();
                                case 3:
                                    throw unauthorized();
                            }
                        }
                        else if (state.firstMsn === 5) {
                            state.error = 1;
                            return '';
                        }

                        const index = genIndex(state);

                        ++state.firstMsn;

                        return index;
                    }
                });

                const problems: Error[] = [];
                const orig = reader.source.fetch.source.isRecoverableUpdateError;
                reader.source.fetch.source.isRecoverableUpdateError = function (err: Error) {

                    problems.push(err);
                    return orig.call(reader.source.fetch.source, err);
                };

                const segments = [];
                const err = await expect((async () => {

                    for await (const obj of reader) {
                        segments.push(obj);
                    }
                })()).to.reject(Error);
                expectCause(err, 'Unauthorized');

                expect(segments.length).to.equal(14);
                expect(problems).to.have.length(4);
                expectCause(problems[0], 'No line data');
                expectCause(problems[1], 'Not Found');
                expectCause(problems[2], 'Service Unavailable');
                expect(problems[3]).to.shallow.equal(err);
            });
        });

        describe('with LL-HLS', () => {

            const prepareLlReader = function (readerOptions: HlsSegmentFetcherOptions & HlsPlaylistFetcherOptions = {}, state: Partial<LlIndexState>, indexGen: ServerState['index']) {

                return prepareLiveReader({
                    lowLatency: true,
                    fullStream: false,
                    ...readerOptions
                }, {
                    partIndex: 0,
                    partCount: 5,
                    index: indexGen,
                    ...state
                });
            };

            it('handles a basic stream', async () => {

                const { reader, state } = prepareLlReader({}, { partIndex: 4, end: { msn: 20, part: 3 } }, (query) => genLlIndex(query, state));

                let updates = 0;
                const incrUpdates = () => updates++;

                const segments = [];
                const expected = { parts: state.partIndex!, gens: 1 };
                for await (const obj of reader) {
                    switch (obj.msn) {
                        case 10:
                            expected.parts = 4;
                            obj.onUpdate = incrUpdates;
                            break;
                        case 11:
                            expected.parts = 0;
                            expected.gens = 2;
                            break;
                    }

                    expect(obj.msn).to.equal(segments.length + 10);
                    expect(obj.entry.parts).to.have.length(expected.parts);
                    expect(obj.hints?.part).to.exist();

                    expect(state.genCount).to.equal(expected.gens);
                    segments.push(obj);

                    expected.gens += 5;
                }

                expect(segments.length).to.equal(11);
                expect(segments[0].entry.parts).to.have.length(5);
                expect(segments[0].hints?.part).to.not.exist();
                expect(segments[0].entry.parts![0].has('byterange')).to.be.false();
                expect(segments[10].entry.parts).to.have.length(3);
                expect(updates).to.equal(1);
            });

            it('finishes partial segments (without another read())', async () => {

                const { reader, state } = prepareLlReader({}, { partIndex: 4, end: { msn: 20, part: 3 } }, (query) => genLlIndex(query, state));

                let updates = 0;
                const incrUpdates = () => updates++;

                const segments = [];
                const expected = { parts: state.partIndex!, gens: 1 };
                for await (const obj of reader) {
                    switch (obj.msn) {
                        case 10:
                            expected.parts = 4;
                            obj.onUpdate = incrUpdates;
                            break;
                        case 11:
                            expected.parts = 0;
                            expected.gens = 2;
                            break;
                    }

                    expect(obj.msn).to.equal(segments.length + 10);
                    expect(obj.entry.parts).to.have.length(expected.parts);
                    expect(obj.hints?.part).to.exist();

                    expect(state.genCount).to.equal(expected.gens);
                    segments.push(obj);

                    expected.gens += 5;

                    await obj.closed();
                }

                expect(segments.length).to.equal(11);
                expect(segments[0].entry.parts).to.have.length(5);
                expect(segments[0].hints?.part).to.not.exist();
                expect(segments[0].entry.parts![0].has('byterange')).to.be.false();
                expect(segments[10].entry.parts).to.have.length(3);
                expect(updates).to.equal(1);
            });

            it('ignores LL parts when lowLatency=false', async () => {

                const { reader, state } = prepareLlReader({ lowLatency: false }, { partIndex: 4, end: { msn: 20, part: 3 } }, (query) => genLlIndex(query, state));

                const segments = [];
                let expectedGens = 1;
                for await (const obj of reader) {
                    expect(obj.msn).to.equal(segments.length + 6);
                    expect(obj.entry.isPartial()).to.be.false();
                    expect(state.genCount).to.equal(expectedGens);
                    segments.push(obj);

                    if (obj.msn > 8) {
                        expectedGens++;
                    }
                }

                expect(segments.length).to.equal(16);
            });

            it('handles a basic stream with initial full segment', async () => {

                const { reader, state } = prepareLlReader({}, { partIndex: 2, end: { msn: 20, part: 3 } }, (query) => genLlIndex(query, state));

                const segments = [];
                const expected = { parts: 5, gens: 1 };
                for await (const obj of reader) {
                    switch (obj.msn) {
                        case 9:
                            expected.parts = 5;
                            break;
                        case 10:
                            expected.parts = 2;
                            expected.gens = 1;
                            break;
                        case 11:
                            expected.parts = 0;
                            expected.gens = 4;
                            break;
                    }

                    expect(obj.msn).to.equal(segments.length + 9);
                    expect(obj.entry.parts).to.have.length(expected.parts);
                    expect(state.genCount).to.equal(expected.gens);
                    segments.push(obj);

                    expected.gens += 5;
                }

                expect(segments.length).to.equal(12);
                expect(segments[0].entry.parts).to.have.length(5);
                expect(segments[11].isClosed).to.be.true();
                expect(segments[11].entry.parts).to.have.length(3);
            });

            it('handles a basic stream using byteranges', async () => {

                const { reader, state } = prepareLlReader({}, { partIndex: 4, end: { msn: 20, part: 3 } }, (query) => {

                    const index = genLlIndex(query, state);
                    const firstMsn = index.media_sequence;
                    let segment;
                    let offset = 0;
                    for (let msn = firstMsn; msn <= index.lastMsn(); ++msn) {     // eslint-disable-line @hapi/for-loop
                        segment = index.getSegment(msn)!;
                        offset = 0;
                        if (segment.parts) {
                            for (let j = 0; j < segment.parts.length; ++j) {
                                const part = segment.parts[j];
                                part.set('uri', `${msn}.ts`, 'string');
                                part.set('byterange', { length: 800 + j, offset: j === 0 ? 0 : undefined }, AttrList.Types.Byterange);
                                offset += 800 + j;
                            }
                        }
                    }

                    if (segment && index.meta.preload_hints) {
                        const hint = index.meta.preload_hints[0];
                        hint.set('uri', `${index.lastMsn() + +!segment.isPartial()}.ts`, AttrList.Types.String);
                        hint.set('byterange-start', segment.isPartial() ? offset : 0, AttrList.Types.Int);
                    }

                    return index;
                });

                const segments = [];
                let expectedParts = 4;
                for await (const obj of reader) {
                    expect(obj.msn).to.equal(segments.length + 10);
                    expect(obj.entry.parts).to.have.length(expectedParts);
                    segments.push(obj);

                    expectedParts = 0;
                }

                expect(segments).to.have.length(11);
                expect(segments[0].entry.parts).to.have.length(5);
                expect(segments[0].entry.parts![0].get('byterange')).to.include('@');
                expect(segments[10].entry.parts).to.have.length(3);
            });

            it('handles active parts being evicted from index', async () => {

                const { reader, state } = prepareLlReader({}, { partIndex: 4, end: { msn: 20, part: 3 } }, (query) => {

                    // Jump during active part

                    if (query._HLS_msn === 13 && query._HLS_part === 2) {
                        state.firstMsn += 5;
                        state.partIndex = 4;
                        query = {};
                    }

                    return genLlIndex(query, state);
                });

                const segments = [];
                const expected = { parts: 5, gens: 1, incr: 5 };
                for await (const obj of reader) {
                    switch (obj.msn) {
                        case 10:
                            expected.parts = 4;
                            break;
                        case 11:
                            expected.parts = 0;
                            expected.gens = 2;
                            break;
                        case 14:
                            (<any>expected).parts = undefined;
                            expected.gens = 15;
                            expected.incr = 0;
                            break;
                        case 16:
                            expected.parts = 5;
                            break;
                        case 18:
                            expected.parts = 4;
                            break;
                        case 19:
                            expected.parts = 0;
                            expected.gens = 16;
                            expected.incr = 5;
                            break;
                    }

                    expect(obj.msn).to.equal(segments.length + 10);
                    if (expected.parts === undefined) {
                        expect(obj.entry.parts).to.not.exist();
                    }
                    else {
                        expect(obj.entry.parts).to.have.length(expected.parts);
                    }

                    expect(state.genCount).to.equal(expected.gens);
                    segments.push(obj);

                    expected.gens += expected.incr;
                }

                expect(segments.length).to.equal(11);
                expect(segments[2].entry.parts).to.have.length(5);
                expect(segments[3].entry.parts).to.not.exist();
                expect(segments[4].entry.parts).to.not.exist();
                expect(segments[5].entry.parts).to.not.exist();
                expect(segments[6].entry.parts).to.have.length(5);
            });

            // TODO: mp4 with initial map
            // TODO: segment jumps
            // TODO: out of index stall
        });

        // handles fullStream option
        // emits index updates
        // TODO: resilience??
    });
});
