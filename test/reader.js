'use strict';

const Fs = require('fs');
const Os = require('os');
const Path = require('path');
const Url = require('url');

const Boom = require('@hapi/boom');
const Code = require('@hapi/code');
const { HlsPlaylistFetcher } = require('hls-playlist-reader');
const Hoek = require('@hapi/hoek');
const Lab = require('@hapi/lab');

const Shared = require('./_shared');
const { HlsSegmentReadable } = require('..');
const { Deferred } = require('hls-playlist-reader/lib/helpers');


// Test shortcuts

const lab = exports.lab = Lab.script();
const { after, before, beforeEach, describe, it } = lab;
const { expect } = Code;


describe('HlsSegmentReadable()', () => {

    const readSegments = Shared.readSegments.bind(null, HlsSegmentReadable);
    let server;

    before(async () => {

        server = await Shared.provisionServer();
        return server.start();
    });

    beforeEach(() => {

        server.onRequest = null;
    });

    after(() => {

        return server.stop();
    });

    describe('constructor', () => {

        it('creates a valid object', async () => {

            const r = new HlsSegmentReadable('http://127.0.0.1:' + server.info.port + '/simple/500.m3u8');

            expect(r).to.be.instanceOf(HlsSegmentReadable);

            await Hoek.wait(10);
            await r.cancel();
        });

        it('throws on missing uri option', () => {

            const createObject = () => {

                return new HlsSegmentReadable();
            };

            expect(createObject).to.throw();
        });

        it('rejects on read() with invalid uri option', async () => {

            const r = new HlsSegmentReadable('asdf://test');
            const reader = r.getReader();

            await expect(reader.read()).to.reject();
            expect(r.locked).to.be.true();
        });
    });

    describe('master index', () => {

        it('does not output any segments', async () => {

            const segments = await readSegments(`http://127.0.0.1:${server.info.port}/simple/index.m3u8`);
            expect(segments).to.have.length(0);
        });

        it('calls "onIndex" hook', async () => {

            let remoteIndex;
            const segments = await readSegments(`http://127.0.0.1:${server.info.port}/simple/index.m3u8`, {
                onIndex(index) {

                    remoteIndex = index;
                }
            });

            expect(segments).to.have.length(0);
            expect(remoteIndex).to.exist();
            expect(remoteIndex.master).to.be.true();
            expect(remoteIndex.variants[0].uri).to.exist();
        });
    });

    describe('on-demand index', () => {

        it('outputs all segments', async () => {

            const segments = await readSegments(`http://127.0.0.1:${server.info.port}/simple/500.m3u8`);

            expect(segments.length).to.equal(3);
            for (let i = 0; i < segments.length; ++i) {
                expect(segments[i].msn).to.equal(i);
            }
        });

        it('emits the "index" event before first read returns', async () => {

            const deferred = new Deferred();
            const readable = new HlsSegmentReadable(`http://127.0.0.1:${server.info.port}/simple/500.m3u8`, {
                onIndex() {

                    deferred.resolve('index');
                }
            });

            const reader = readable.getReader();
            const winner = await Promise.race([reader.read(), deferred.promise]);

            expect(winner).to.be.equal('index');
        });

        it('supports the startDate option', async () => {

            const r = new HlsSegmentReadable(`http://127.0.0.1:${server.info.port}/simple/500.m3u8`, { startDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });
            const segments = [];

            for await (const segment of r) {
                expect(segment.msn).to.equal(segments.length + 2);
                segments.push(segment);
            }

            expect(segments).to.have.length(1);
        });

        it('supports the stopDate option', async () => {

            const r = new HlsSegmentReadable(`http://127.0.0.1:${server.info.port}/simple/500.m3u8`, { stopDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });
            const segments = [];

            for await (const segment of r) {
                expect(segment.msn).to.equal(segments.length);
                segments.push(segment);
            }

            expect(segments).to.have.length(2);
        });

        it('applies the extensions option', async () => {

            const extensions = {
                '#EXT-MY-HEADER': false,
                '#EXT-MY-SEGMENT-OK': true
            };

            let index;
            const r = new HlsSegmentReadable('file://' + Path.join(__dirname, 'fixtures', '500.m3u8'), { extensions,
                onIndex(newIndex) {

                    index = newIndex;
                }
            });

            const segments = [];
            for await (const segment of r) {
                segments.push(segment);
            }

            expect(index).to.exist();
            expect(index.vendor[0]).to.equal(['#EXT-MY-HEADER', 'hello']);
            expect(index.segments[1].vendor[0]).to.equal(['#EXT-MY-SEGMENT-OK', null]);
            expect(segments).to.have.length(3);
            expect(segments[1].entry.vendor[0]).to.equal(['#EXT-MY-SEGMENT-OK', null]);
        });

        it('does not internally buffer', async () => {

            let nextCalls = 0;
            const r = new HlsSegmentReadable('file://' + Path.join(__dirname, 'fixtures', 'long.m3u8'));
            const orig = r.fetch.next;
            r.fetch.next = (opts) => {

                ++nextCalls;
                return orig.call(r.fetch, opts);
            };

            let count = 0;
            for await (const obj of r) {
                ++count;
                expect(obj).to.exist();
                expect(nextCalls - count).to.equal(0);
                await Hoek.wait(1);
                expect(nextCalls - count).to.equal(0);
            }
        });

        /*it('supports the highWaterMark option', async () => {

            const r = new HlsSegmentReadable('file://' + Path.join(__dirname, 'fixtures', 'long.m3u8'), { highWaterMark: 2 });
            const buffered = [];

            for await (const obj of r) {
                expect(obj).to.exist();
                await Hoek.wait(20);
                buffered.push(r._readableState.buffer.length);
            }

            expect(buffered).to.equal([2, 2, 2, 2, 1, 0]);
        });*/

        it('can be cancelled', async () => {

            let cancelled = false;
            const r = new HlsSegmentReadable('file://' + Path.join(__dirname, 'fixtures', '500.m3u8'));
            const orig = r.fetch.cancel;
            r.fetch.cancel = (err) => {

                cancelled = true;
                return orig.call(r.fetch, err);
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

    describe('live index', { parallel: false }, () => {

        const serverState = { state: {} };
        let liveServer;

        const prepareLiveReader = function (readerOptions = {}, state = {}) {

            const reader = new HlsSegmentReadable(`http://127.0.0.1:${liveServer.info.port}/live/live.m3u8`, { fullStream: true, ...readerOptions });
            reader.fetch.source._intervals = [];
            reader.fetch.source.getUpdateInterval = function (...args) {

                this._intervals.push(HlsPlaylistFetcher.prototype.getUpdateInterval.call(this, ...args));
                return undefined;
            };

            serverState.state = { firstMsn: 0, segmentCount: 10, targetDuration: 2, ...state };

            return { reader, state: serverState.state };
        };

        before(() => {

            liveServer = Shared.provisionLiveServer(serverState);
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

        it('handles a basic stream (file)', async () => {

            const state = serverState.state = { firstMsn: 0, segmentCount: 10, targetDuration: 10 };

            const tmpDir = await Fs.promises.mkdtemp(await Fs.promises.realpath(Os.tmpdir()) + Path.sep);
            try {
                const tmpUrl = new URL('next.m3u8', Url.pathToFileURL(tmpDir + Path.sep));
                const indexUrl = new URL('index.m3u8', Url.pathToFileURL(tmpDir + Path.sep));
                await Fs.promises.writeFile(indexUrl, Shared.genIndex(state).toString(), 'utf-8');

                const reader = new HlsSegmentReadable(indexUrl.href, { fullStream: true });
                const segments = [];

                (async () => {

                    while (!state.ended) {
                        await Hoek.wait(50);

                        state.firstMsn++;
                        if (state.firstMsn === 5) {
                            state.ended = true;
                        }

                        // Atomic write

                        await Fs.promises.writeFile(tmpUrl, Shared.genIndex(state).toString(), 'utf-8');
                        await Fs.promises.rename(tmpUrl, indexUrl);
                    }
                })();

                for await (const obj of reader) {
                    expect(obj.msn).to.equal(segments.length);
                    segments.push(obj);
                }

                expect(segments).to.have.length(15);
            }
            finally {
                await Fs.promises.rm(tmpDir, { recursive: true });
            }
        });

        it('can start with 0 segments', async () => {

            const { reader, state } = prepareLiveReader({}, { segmentCount: 0, index() {

                const index = Shared.genIndex(state);
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

            const playlist = await reader.fetch._requestPlaylistUpdate();
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

                    return Shared.genIndex(state);
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

                    const index = Shared.genIndex(state);

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

            const errors = [];
            const { reader, state } = prepareLiveReader({ onProblem: errors.push.bind(errors) }, {
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

            const segments = [];
            for await (const obj of reader) {
                expect(obj.msn).to.equal(segments.length);
                segments.push(obj);
            }

            expect(segments).to.have.length(30);
            expect(errors.length).to.be.greaterThan(0);
            expect(errors[0]).to.be.an.error('Internal Server Error');
        });

        it('drops segments when reader is slow', async () => {

            const { reader, state } = prepareLiveReader({ fullStream: false }, {
                index() {

                    if (state.firstMsn === 50) {
                        state.ended = true;
                    }

                    const index = Shared.genIndex(state);

                    return index;
                }
            });

            const segments = [];
            for await (const obj of reader) {
                segments.push(obj);

                state.firstMsn += 5;

                await Hoek.wait(5);
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
                            await Hoek.wait(100);
                        }

                        return Shared.genIndex(state);
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
                            await Hoek.wait(10);
                        }

                        return Shared.genIndex(state);
                    }
                });

                let sourceReason;
                const orig = reader.fetch.source.cancel;
                reader.fetch.source.cancel = (reason) => {

                    sourceReason = reason;
                    return orig.call(reader.fetch.source, reason);
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

                expect(sourceReason.message).to.equal('destroyed');
            });
        });

        // TODO: move
        describe('isRecoverableUpdateError()', () => {

            it('is called on index update errors', async () => {

                const { reader, state } = prepareLiveReader({}, {
                    index() {

                        const { error } = state;
                        if (error) {
                            state.error++;
                            switch (error) {
                                case 1:
                                case 2:
                                case 3:
                                    throw Boom.notFound();
                                case 4:
                                    throw Boom.serverUnavailable();
                                case 5:
                                    throw Boom.unauthorized();
                            }
                        }
                        else if (state.firstMsn === 5) {
                            state.error = 1;
                            return '';
                        }

                        const index = Shared.genIndex(state);

                        ++state.firstMsn;

                        return index;
                    }
                });

                const errors = [];
                const orig = reader.fetch.source.isRecoverableUpdateError;
                reader.fetch.source.isRecoverableUpdateError = function (err) {

                    errors.push(err);
                    return orig.call(reader.fetch.source, err);
                };

                const segments = [];
                const err = await expect((async () => {

                    for await (const obj of reader) {
                        segments.push(obj);
                    }
                })()).to.reject('Unauthorized');

                expect(segments.length).to.equal(14);
                expect(errors).to.have.length(4);
                expect(errors[0]).to.have.error('No line data');
                expect(errors[1]).to.have.error(Boom.Boom, 'Not Found');
                expect(errors[2]).to.have.error(Boom.Boom, 'Service Unavailable');
                expect(errors[3]).to.shallow.equal(err);
            });
        });

        describe('with LL-HLS', () => {

            const prepareLlReader = function (readerOptions = {}, state = {}, indexGen) {

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

            const { genLlIndex } = Shared;

            it('handles a basic stream', async () => {

                const { reader, state } = prepareLlReader({}, { partIndex: 4, end: { msn: 20, part: 3 } }, (query) => genLlIndex(query, state));

                let updates = 0;
                const incrUpdates = () => updates++;

                const segments = [];
                const expected = { parts: state.partIndex, gens: 1 };
                for await (const obj of reader) {
                    switch (obj.msn) {
                        case 10:
                            expected.parts = 4;
                            obj.onUpdate = incrUpdates;
                            break;
                        case 11:
                            expected.parts = 1;
                            expected.gens = 3;
                            break;
                    }

                    expect(obj.msn).to.equal(segments.length + 10);
                    expect(obj.entry.parts).to.have.length(expected.parts);
                    expect(obj.entry.parts[0].has('byterange')).to.be.false();
                    expect(state.genCount).to.equal(expected.gens);
                    //expect(reader.hints.part).to.exist();
                    segments.push(obj);

                    expected.gens += 5;
                }

                expect(segments.length).to.equal(11);
                expect(segments[0].entry.parts).to.have.length(5);
                expect(segments[10].entry.parts).to.have.length(3);
                expect(updates).to.equal(1);
                //expect(reader.hints.part).to.not.exist();
            });

            it('finishes partial segments (without another read())', async () => {

                const { reader, state } = prepareLlReader({}, { partIndex: 4, end: { msn: 20, part: 3 } }, (query) => genLlIndex(query, state));

                let updates = 0;
                const incrUpdates = () => updates++;

                const segments = [];
                const expected = { parts: state.partIndex, gens: 1 };
                for await (const obj of reader) {
                    switch (obj.msn) {
                        case 10:
                            expected.parts = 4;
                            obj.onUpdate = incrUpdates;
                            break;
                        case 11:
                            expected.parts = 1;
                            expected.gens = 3;
                            break;
                    }

                    expect(obj.msn).to.equal(segments.length + 10);
                    expect(obj.entry.parts).to.have.length(expected.parts);
                    expect(obj.entry.parts[0].has('byterange')).to.be.false();
                    expect(state.genCount).to.equal(expected.gens);
                    //expect(reader.hints.part).to.exist();
                    segments.push(obj);

                    expected.gens += 5;

                    await obj.closed();
                }

                expect(segments.length).to.equal(11);
                expect(segments[0].entry.parts).to.have.length(5);
                expect(segments[10].entry.parts).to.have.length(3);
                expect(updates).to.equal(1);
                //expect(reader.hints.part).to.not.exist();
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
                            expected.parts = 1;
                            expected.gens = 5;
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
                    let offset;
                    for (let msn = firstMsn; msn <= index.lastMsn(); ++msn) {     // eslint-disable-line @hapi/for-loop
                        segment = index.getSegment(msn);
                        offset = 0;
                        if (segment.parts) {
                            for (let j = 0; j < segment.parts.length; ++j) {
                                const part = segment.parts[j];
                                part.set('uri', `${msn}.ts`, 'string');
                                part.set('byterange', { length: 800 + j, offset: j === 0 ? 0 : undefined }, 'byterange');
                                offset += 800 + j;
                            }
                        }
                    }

                    if (index.meta.preload_hints) {
                        const hint = index.meta.preload_hints[0];
                        hint.set('uri', `${index.lastMsn() + +!segment.isPartial()}.ts`, 'string');
                        hint.set('byterange-start', segment.isPartial() ? offset : 0, 'int');
                    }

                    return index;
                });

                const segments = [];
                let expectedParts = 4;
                for await (const obj of reader) {
                    expect(obj.msn).to.equal(segments.length + 10);
                    expect(obj.entry.parts).to.have.length(expectedParts);
                    expect(obj.entry.parts[0].get('byterange')).to.include('@');
                    segments.push(obj);

                    expectedParts = 1;
                }

                expect(segments).to.have.length(11);
                expect(segments[0].entry.parts).to.have.length(5);
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
                            expected.parts = 1;
                            expected.gens = 3;
                            break;
                        case 14:
                            expected.parts = undefined;
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
                            expected.parts = 1;
                            expected.gens = 17;
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
                expect(segments[3].entry.parts).to.have.length(2);
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
