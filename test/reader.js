'use strict';

const Fs = require('fs');
const Path = require('path');
const Readable = require('stream').Readable;

const Boom = require('@hapi/boom');
const Code = require('@hapi/code');
const Hapi = require('@hapi/hapi');
const Hoek = require('@hapi/hoek');
const Inert = require('@hapi/inert');
const Joi = require('joi');
const Lab = require('@hapi/lab');
const M3U8Parse = require('m3u8parse');

const createSimpleReader = require('..');
const { HlsSegmentReader } = require('..');


// Declare internals

const internals = {
    checksums: [
        'a6b0e0ce44f29e965e751113b39fdf4a47787cab',
        'c38d0718851a20be2edba13fc1643c1076826c62',
        '612991f34ae7cc19df5d595a2a4249b8f5d2d3f0',
        'bc600f4039aae412c4d978b3fd4d608ce4dec59a'
    ]
};


internals.provisionServer = () => {

    const server = new Hapi.Server({
        routes: { files: { relativeTo: Path.join(__dirname, 'fixtures') } },
        debug: false
    });

    server.register(Inert);

    const delay = async (request, h) => {

        await Hoek.wait(200);

        return 200;
    };

    const slowServe = (request, h) => {

        const slowStream = new Readable();
        slowStream._read = () => { };

        const path = Path.join(__dirname, 'fixtures', request.params.path);
        const buffer = Fs.readFileSync(path);
        slowStream.push(buffer.slice(0, 5000));
        setTimeout(() => {

            slowStream.push(buffer.slice(5000));
            slowStream.push(null);
        }, 200);

        return h.response(slowStream).type('video/mp2t');
    };

    server.route({ method: 'GET', path: '/simple/{path*}', handler: { directory: { path: '.' } } });
    server.route({ method: 'GET', path: '/slow/{path*}', handler: { directory: { path: '.' } }, config: { pre: [{ method: delay, assign: 'delay' }] } });
    server.route({ method: 'GET', path: '/slow-data/{path*}', handler: slowServe });
    server.route({
        method: 'GET', path: '/error', handler(request, h) {

            throw new Error('!!!');
        }
    });

    return server;
};


internals.readSegments = (Class, ...args) => {

    let r;
    const promise = new Promise((resolve, reject) => {

        r = new Class(...args);
        r.on('error', reject);

        const segments = [];
        r.on('data', (segment) => {

            segments.push(segment);
        });

        r.on('end', () => {

            resolve(segments);
        });
    });

    promise.reader = r;

    return promise;
};



// Test shortcuts

const lab = exports.lab = Lab.script();
const { after, afterEach, before, beforeEach, describe, it } = lab;
const { expect } = Code;


describe('HlsSegmentReader()', () => {

    const readSegments = internals.readSegments.bind(null, HlsSegmentReader);
    let server;

    before(async () => {

        server = await internals.provisionServer();
        return server.start();
    });

    after(() => {

        return server.stop();
    });

    describe('constructor', () => {

        it('creates a valid object', () => {

            const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.m3u8');

            expect(r).to.be.instanceOf(HlsSegmentReader);

            r.destroy();
        });

        it('throws on missing uri option', () => {

            const createObject = () => {

                return new HlsSegmentReader();
            };

            expect(createObject).to.throw();
        });

        it('throws on invalid uri option', () => {

            const createObject = () => {

                return new HlsSegmentReader('asdf://test');
            };

            expect(createObject).to.throw();
        });
    });

    it('emits error on missing remote host', async () => {

        const promise = readSegments('http://does.not.exist/simple/500.m3u8');
        await expect(promise).to.reject(Error, /getaddrinfo ENOTFOUND does\.not\.exist/);
    });

    it('emits error for missing data', async () => {

        const promise = readSegments(`http://localhost:${server.info.port}/notfound`);
        await expect(promise).to.reject(Error, /Not Found/);
    });

    it('emits error for http error responses', async () => {

        const promise = readSegments(`http://localhost:${server.info.port}/error`);
        await expect(promise).to.reject(Error, /Internal Server Error/);
    });

    it('emits error on non-index responses', async () => {

        const promise = readSegments(`http://localhost:${server.info.port}/simple/500.ts`);
        await expect(promise).to.reject(Error, /Invalid MIME type/);
    });

    it('emits error on malformed index files', async () => {

        const promise = readSegments(`http://localhost:${server.info.port}/simple/malformed.m3u8`);
        await expect(promise).to.reject(M3U8Parse.ParserError);
    });

    describe('master index', () => {

        it('does not output any segments', async () => {

            const segments = await readSegments(`http://localhost:${server.info.port}/simple/index.m3u8`);
            expect(segments.length).to.equal(0);
        });

        it('emits "index" event', async () => {

            const promise = readSegments(`http://localhost:${server.info.port}/simple/index.m3u8`);

            let remoteIndex;
            promise.reader.on('index', (index) => {

                remoteIndex = index;
            });

            await promise;

            expect(remoteIndex).to.exist();
            expect(remoteIndex.master).to.be.true();
            expect(remoteIndex.variants[0].uri).to.exist();
        });
    });

    describe('on-demand index', () => {

        it('outputs all segments', async () => {

            const segments = await readSegments(`http://localhost:${server.info.port}/simple/500.m3u8`);

            expect(segments.length).to.equal(3);
            for (let i = 0; i < segments.length; ++i) {
                expect(segments[i].msn).to.equal(i);
            }
        });

        it('emits the "index" event before starting', async () => {

            const promise = readSegments(`http://localhost:${server.info.port}/simple/500.m3u8`);

            let hasSegment = false;
            promise.reader.on('data', () => {

                hasSegment = true;
            });

            const index = await new Promise((resolve) => {

                promise.reader.on('index', resolve);
            });

            expect(index).to.exist();
            expect(hasSegment).to.be.false();

            await promise;

            expect(hasSegment).to.be.true();
        });

        it('supports the startDate option', async () => {

            const r = new HlsSegmentReader(`http://localhost:${server.info.port}/simple/500.m3u8`, { startDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });
            const segments = [];

            for await (const segment of r) {
                expect(segment.msn).to.equal(segments.length + 2);
                segments.push(segment);
            }

            expect(segments).to.have.length(1);
        });

        it('supports the stopDate option', async () => {

            const r = new HlsSegmentReader(`http://localhost:${server.info.port}/simple/500.m3u8`, { stopDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });
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

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', '500.m3u8'), { extensions });
            const segments = [];

            for await (const obj of r) {
                segments.push(obj);
            }

            expect(r.index).to.exist();
            expect(r.index.vendor[0]).to.equal(['#EXT-MY-HEADER', 'hello']);
            expect(r.index.segments[1].vendor[0]).to.equal(['#EXT-MY-SEGMENT-OK', null]);
            expect(segments).to.have.length(3);
            expect(segments[1].entry.vendor[0]).to.equal(['#EXT-MY-SEGMENT-OK', null]);
        });

        it('does not internally buffer (highWaterMark=0)', async () => {

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'long.m3u8'));

            for await (const obj of r) {
                expect(obj).to.exist();
                await Hoek.wait(20);
                expect(r._readableState.buffer).to.have.length(0);
            }
        });

        /*it('supports the highWaterMark option', async () => {

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'long.m3u8'), { highWaterMark: 2 });
            const buffered = [];

            for await (const obj of r) {
                expect(obj).to.exist();
                await Hoek.wait(20);
                buffered.push(r._readableState.buffer.length);
            }

            expect(buffered).to.equal([2, 2, 2, 2, 1, 0]);
        });*/

        it('can be destroyed', async () => {

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', '500.m3u8'));
            const segments = [];

            for await (const obj of r) {
                segments.push(obj);
                r.destroy();
            }

            expect(segments).to.have.length(1);
        });

        // handles all kinds of segment reference url
        // handles .m3u files
    });

    describe('live index', { parallel: false }, () => {

        let serverState = {};
        let liveServer;

        const prepareLiveReader = function (readerOptions = {}, state = {}) {

            const reader = new HlsSegmentReader(`http://localhost:${liveServer.info.port}/live/live.m3u8`, { fullStream: true, ...readerOptions });
            reader._intervals = [];
            reader._getUpdateInterval = function (updated) {

                this._intervals.push(HlsSegmentReader.prototype._getUpdateInterval.call(this, updated));
                return undefined;
            };

            serverState = { firstSeqNo: 0, segmentCount: 10, targetDuration: 2, ...state };

            return { reader, state: serverState };
        };

        const genIndex = function ({ targetDuration, segmentCount, firstSeqNo, partCount, partIndex, ended }) {

            const partDuration = targetDuration / partCount;

            const segments = [];
            const meta = {};

            for (let i = 0; i < segmentCount; ++i) {
                const parts = [];
                if (i >= segmentCount - 2) {
                    for (let j = 0; j < partCount; ++j) {
                        parts.push(new M3U8Parse.AttrList({
                            duration: partDuration,
                            uri: `"${firstSeqNo + i}-part${j}.ts"`
                        }));
                    }
                }

                segments.push({
                    duration: targetDuration || 2,
                    uri: `${firstSeqNo + i}.ts`,
                    title: '',
                    parts: parts.length ? parts : undefined
                });
            }

            if (partIndex !== undefined) {
                if (partIndex > 0) {
                    const parts = [];
                    for (let i = 0; i < partIndex; ++i) {
                        parts.push(new M3U8Parse.AttrList({
                            duration: partDuration,
                            uri: `"${firstSeqNo + segmentCount}-part${i}.ts"`
                        }));
                    }

                    segments.push({ parts });
                }

                // Add hint

                meta.preload_hints = [new M3U8Parse.AttrList({
                    type: 'part',
                    uri: `"${firstSeqNo + segmentCount}-part${partIndex}.ts"`
                })];
            }

            const index = new M3U8Parse.M3U8Playlist({
                first_seq_no: firstSeqNo,
                target_duration: targetDuration,
                part_info: partCount ? new M3U8Parse.AttrList({ 'part-target': partDuration }) : undefined,
                segments,
                meta,
                ended
            });

            //console.log('GEN', index.segments[index.segments.length - 1].parts, index.meta.preload_hints)

            return index;
        };

        beforeEach(() => {

            liveServer = new Hapi.Server({
                routes: {
                    files: { relativeTo: Path.join(__dirname, 'fixtures') }
                }
            });

            const serveLiveIndex = async (request, h) => {

                let index;
                if (serverState.index) {
                    index = await serverState.index(request.query);
                }
                else {
                    index = genIndex(serverState);
                }

                return h.response(index.toString()).type('application/vnd.apple.mpegURL');
            };

            const serveSegment = (request, h) => {

                if (serverState.slow) {
                    const slowStream = new Readable();
                    slowStream._read = () => {};

                    slowStream.push(Buffer.alloc(5000));

                    return h.response(slowStream).type('video/mp2t').bytes(30000);
                }

                const size = ~~(5000 / (request.params.part === undefined ? 1 : serverState.partCount)) + parseInt(request.params.msn) + 100 * parseInt(request.params.part || 0);
                return h.response(Buffer.alloc(size)).type('video/mp2t').bytes(size);
            };

            liveServer.route({
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
            liveServer.route({ method: 'GET', path: '/live/{msn}.ts', handler: serveSegment });
            liveServer.route({ method: 'GET', path: '/live/{msn}-part{part}.ts', handler: serveSegment });

            return liveServer.start();
        });

        afterEach(() => {

            return liveServer.stop();
        });

        it('handles a basic stream', async () => {

            const { reader, state } = prepareLiveReader();
            const segments = [];

            for await (const obj of reader) {
                expect(obj.msn).to.equal(segments.length);
                segments.push(obj);

                if (obj.msn > 5) {
                    state.firstSeqNo++;
                    if (state.firstSeqNo === 5) {
                        state.ended = true;
                        await Hoek.wait(50);
                    }
                }
            }

            expect(segments).to.have.length(15);
        });

        it('handles sequence number resets', async () => {

            const { reader, state } = prepareLiveReader({}, { firstSeqNo: 10 });
            const segments = [];
            let reset = false;

            for await (const obj of reader) {
                segments.push(obj);

                if (!reset) {
                    state.firstSeqNo++;

                    if (state.firstSeqNo === 16) {
                        state.firstSeqNo = 0;
                        state.segmentCount = 1;
                        reset = true;

                        await Hoek.wait(50);
                    }
                }
                else {
                    state.segmentCount++;
                    if (state.segmentCount === 5) {
                        state.ended = true;
                        await Hoek.wait(50);
                    }
                }
            }

            expect(segments).to.have.length(11);
            expect(segments[6].msn).to.equal(0);
            expect(segments[5].entry.discontinuity).to.be.false();
            expect(segments[6].entry.discontinuity).to.be.true();
            expect(segments[7].entry.discontinuity).to.be.false();
        });

        it('handles sequence number jumps', async () => {

            const { reader, state } = prepareLiveReader();
            const segments = [];
            let skipped = false;

            for await (const obj of reader) {
                segments.push(obj);

                if (!skipped && obj.msn >= state.segmentCount - 1) {
                    state.firstSeqNo++;
                    if (state.firstSeqNo === 5) {
                        state.firstSeqNo = 50;
                        skipped = true;
                    }
                }

                if (skipped && obj.msn > 55) {
                    state.firstSeqNo++;
                    if (state.firstSeqNo === 55) {
                        state.ended = true;
                        await Hoek.wait(50);
                    }
                }
            }

            expect(segments).to.have.length(29);
            expect(segments[14].msn).to.equal(50);
            expect(segments[13].entry.discontinuity).to.be.false();
            expect(segments[14].entry.discontinuity).to.be.true();
            expect(segments[15].entry.discontinuity).to.be.false();
        });

        it('handles a temporary server outage', async () => {

            const { reader, state } = prepareLiveReader({}, {
                index() {

                    if (state.error === undefined && state.firstSeqNo === 5) {
                        state.error = 6;
                    }

                    if (state.error) {
                        --state.error;
                        ++state.firstSeqNo;
                        throw new Error('fail');
                    }

                    if (state.firstSeqNo === 20) {
                        state.ended = true;
                    }

                    const index = genIndex(state);

                    ++state.firstSeqNo;

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

            expect(segments).to.have.length(30);
            expect(errors.length).to.be.greaterThan(0);
            expect(errors[0]).to.be.an.error('Internal Server Error');
        });

        it('drops segments when reader is slow', async () => {

            const { reader, state } = prepareLiveReader({ fullStream: false }, {
                index() {

                    if (state.firstSeqNo === 50) {
                        state.ended = true;
                    }

                    return genIndex(state);
                }
            });

            const segments = [];
            for await (const obj of reader) {
                segments.push(obj);

                state.firstSeqNo += 5;
                await Hoek.wait(10);
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

        describe('destroy()', () => {

            it('works when called while waiting for a segment', async () => {

                const { reader, state } = prepareLiveReader({ fullStream: false }, {
                    async index() {

                        if (state.firstSeqNo > 0) {
                            await Hoek.wait(100);
                        }

                        return genIndex(state);
                    }
                });

                setTimeout(() => reader.destroy(), 50);

                const segments = [];
                for await (const obj of reader) {
                    segments.push(obj);

                    state.firstSeqNo++;
                }

                expect(segments).to.have.length(4);
            });

            it('emits passed error', async () => {

                const { reader, state } = prepareLiveReader({ fullStream: false }, {
                    async index() {

                        if (state.firstSeqNo > 0) {
                            await Hoek.wait(10);
                        }

                        return genIndex(state);
                    }
                });

                setTimeout(() => reader.destroy(new Error('destroyed')), 50);

                await expect((async () => {

                    for await (const {} of reader) {
                        state.firstSeqNo++;
                    }
                })()).to.reject('destroyed');
            });
        });

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
                        else if (state.firstSeqNo === 5) {
                            state.error = 1;
                            return '';
                        }

                        const index = genIndex(state);

                        ++state.firstSeqNo;

                        return index;
                    }
                });

                const errors = [];
                reader.isRecoverableUpdateError = function (err) {

                    errors.push(err);
                    return HlsSegmentReader.prototype.isRecoverableUpdateError.call(reader, err);
                };

                const segments = [];
                const err = await expect((async () => {

                    for await (const obj of reader) {
                        segments.push(obj);
                    }
                })()).to.reject('Unauthorized');

                expect(segments.length).to.equal(14);
                expect(errors).to.have.length(4);
                expect(errors[0]).to.have.error(M3U8Parse.ParserError, 'Missing required #EXTM3U header');
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

            const genLlIndex = function (query, state) {

                // Return playlist with exactly the next part

                if (!state.ended && query._HLS_msn !== undefined) {
                    let msn = query._HLS_msn;
                    let part = query._HLS_part === undefined ? state.partCount : query._HLS_part + 1;

                    if (part >= state.partCount) {
                        msn++;
                        part = 0;
                    }

                    state.firstSeqNo = msn - state.segmentCount;
                    state.partIndex = part;
                }

                const index = genIndex(state);

                index.server_control = new M3U8Parse.AttrList({
                    'can-block-reload': 'YES',
                    'part-hold-back': 3 * state.targetDuration / state.partCount
                });

                state.genCount = (state.genCount || 0) + 1;

                if (!state.ended) {
                    if (state.end &&
                        index.lastSeqNo() > state.end.msn || (index.lastSeqNo() === state.end.msn && state.end.part === index.getSegment(index.lastSeqNo()).parts.length)) {

                        index.ended = state.ended = true;
                        delete index.meta.preload_hints;
                        return index;
                    }

                    state.partIndex = ~~state.partIndex + 1;
                    if (state.partIndex >= state.partCount) {
                        state.partIndex = 0;
                        state.firstSeqNo++;
                    }
                }

                return index;
            };

            it('handles a basic stream', async () => {

                const hints = [];
                const { reader, state } = prepareLlReader({}, { partIndex: 4, end: { msn: 20, part: 3 } }, (query) => genLlIndex(query, state));

                reader.on('hint', (hint) => hints.push(hint));

                const segments = [];
                const expected = { parts: state.partIndex, gens: 1 };
                for await (const obj of reader) {
                    switch (obj.msn) {
                        case 10:
                            expected.parts = 4;
                            break;
                        case 11:
                            expected.parts = 1;
                            expected.gens = 3;
                            break;
                    }

                    expect(obj.msn).to.equal(segments.length + 10);
                    expect(obj.entry.parts).to.have.length(expected.parts);
                    expect(obj.entry.parts[0].has('byterange')).to.be.false();
                    expect(hints.length).to.equal(expected.gens);
                    expect(state.genCount).to.equal(expected.gens);
                    segments.push(obj);

                    expected.gens += 5;
                }

                expect(segments.length).to.equal(11);
                expect(segments[0].entry.parts).to.have.length(5);
                expect(segments[10].entry.parts).to.have.length(3);
            });

            it('updates index outside read()', async () => {

                const hints = [];
                const { reader, state } = prepareLlReader({}, { partIndex: 4, end: { msn: 25, part: 3 } }, (query) => genLlIndex(query, state));

                reader.on('hint', (hint) => hints.push(hint));

                const segments = [];
                const expected = { parts: state.partIndex, gens: 75 };
                for await (const obj of reader) {
                    switch (obj.msn) {
                        case 10:
                            await Hoek.wait(500);         // Allow time for update loop to complete
                            break;
                    }

                    expect(hints.length).to.equal(expected.gens - 1);
                    expect(state.genCount).to.equal(expected.gens);
                    segments.push(obj);
                }

                expect(segments.length).to.equal(12);
                expect(segments[0].msn).to.equal(10);
                expect(segments[0].entry.parts).to.have.length(5);
                expect(segments[1].msn).to.equal(15);
                expect(segments[1].entry.parts).to.not.exist();
                expect(segments[11].msn).to.equal(25);
                expect(segments[11].entry.parts).to.have.length(3);
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
                expect(segments[11].entry.parts).to.have.length(3);
            });

            it('handles a basic stream using byteranges', async () => {

                const { reader, state } = prepareLlReader({}, { partIndex: 4, end: { msn: 20, part: 3 } }, (query) => {

                    const index = genLlIndex(query, state);
                    const firstMsn = index.first_seq_no;
                    let segment;
                    let offset;
                    for (let msn = firstMsn; msn <= index.lastSeqNo(); ++msn) {     // eslint-disable-line @hapi/hapi/for-loop
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
                        hint.set('uri', `${index.lastSeqNo() + +!segment.isPartial()}.ts`, 'string');
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

                expect(segments.length).to.equal(11);
                expect(segments[0].entry.parts).to.have.length(5);
                expect(segments[10].entry.parts).to.have.length(3);
            });

            it('handles active parts being evicted from index', async () => {

                const { reader, state } = prepareLlReader({}, { partIndex: 4, end: { msn: 20, part: 3 } }, (query) => {

                    // Jump during active part

                    if (query._HLS_msn === 13 && query._HLS_part === 2) {
                        state.firstSeqNo += 5;
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
