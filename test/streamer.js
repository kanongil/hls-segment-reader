'use strict';

const Crypto = require('crypto');
const Fs = require('fs');
const Path = require('path');
const Readable = require('stream').Readable;

const Code = require('@hapi/code');
const Hapi = require('@hapi/hapi');
const Hoek = require('@hapi/hoek');
const Inert = require('@hapi/inert');
const Lab = require('@hapi/lab');
const Uristream = require('uristream');

const Shared = require('./_shared');

// eslint-disable-next-line @hapi/hapi/capitalize-modules
const createSimpleReader = require('..');
const { HlsSegmentReader, HlsSegmentStreamer } = require('..');


// Declare internals

const internals = {
    checksums: [
        'a6b0e0ce44f29e965e751113b39fdf4a47787cab',
        'c38d0718851a20be2edba13fc1643c1076826c62',
        '612991f34ae7cc19df5d595a2a4249b8f5d2d3f0',
        'bc600f4039aae412c4d978b3fd4d608ce4dec59a'
    ]
};


// Test shortcuts

const lab = exports.lab = Lab.script();
const { after, afterEach, before, beforeEach, describe, it } = lab;
const { expect } = Code;


describe('HlsSegmentStreamer()', () => {

    const readSegments = Shared.readSegments.bind(null, HlsSegmentStreamer);
    let server;

    before(async () => {

        server = await Shared.provisionServer();
        return server.start();
    });

    after(() => {

        return server.stop();
    });

    describe('constructor', () => {

        it('creates a valid object', () => {

            const r = new HlsSegmentStreamer();

            expect(r).to.be.instanceOf(HlsSegmentStreamer);

            r.destroy();
        });

        it('handles a reader', () => {

            const r = new HlsSegmentStreamer(new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.m3u8'));

            expect(r).to.be.instanceOf(HlsSegmentStreamer);

            r.destroy();
        });
    });

    it('emits error for missing data', async () => {

        const promise = readSegments(new HlsSegmentReader(`http://localhost:${server.info.port}/notfound`));
        await expect(promise).to.reject(Error, /Not Found/);
    });

    // TODO: move??
    it('Uristream maps file extensions to suitable mime types', async () => {

        const map = new Map([
            ['audio.aac', 'audio/x-aac'],
            ['audio.ac3', 'audio/ac3'],
            ['audio.dts', 'audio/vnd.dts'],
            ['audio.eac3', 'audio/eac3'],
            ['audio.m4a', 'audio/mp4'],
            ['file.m4s', 'video/iso.segment'],
            ['file.mp4', 'video/mp4'],
            ['text.vtt', 'text/vtt'],
            ['video.m4v', 'video/x-m4v']
        ]);

        for (const [file, mime] of map) {
            const meta = await new Promise((resolve, reject) => {

                const uristream = new Uristream(`file://${Path.resolve(__dirname, 'fixtures/files', file)}`);
                uristream.on('error', reject);
                uristream.on('meta', resolve);
            });

            expect(meta.mime).to.equal(mime);
        }
    });

    it('emits error on unknown segment mime type', async () => {

        await expect((async () => {

            const r = createSimpleReader('file://' + Path.join(__dirname, 'fixtures', 'badtype.m3u8'), { withData: false });

            for await (const obj of r) {

                expect(obj).to.exist();
            }
        })()).to.reject(Error, /Unsupported segment MIME type/);

        await expect((async () => {

            const r = createSimpleReader('file://' + Path.join(__dirname, 'fixtures', 'badtype-data.m3u8'), { withData: true });

            for await (const obj of r) {

                expect(obj).to.exist();
            }
        })()).to.reject(Error, /Unsupported segment MIME type/);
    });

    describe('master index', () => {

        it('does not output any segments', async () => {

            const segments = await readSegments(new HlsSegmentReader(`http://localhost:${server.info.port}/simple/index.m3u8`));
            expect(segments.length).to.equal(0);
        });

        it('emits "index" event', async () => {

            const promise = readSegments(new HlsSegmentReader(`http://localhost:${server.info.port}/simple/index.m3u8`));

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

            const segments = await readSegments(new HlsSegmentReader(`http://localhost:${server.info.port}/simple/500.m3u8`));

            expect(segments).to.have.length(3);
            for (let i = 0; i < segments.length; ++i) {
                expect(segments[i].segment.msn).to.equal(i);
            }
        });

        it('handles byte-range (file)', async () => {

            const r = createSimpleReader('file://' + Path.join(__dirname, 'fixtures', 'single.m3u8'), { withData: true });
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
            }

            expect(checksums).to.equal(internals.checksums);
        });

        it('handles byte-range (http)', async () => {

            const r = createSimpleReader(`http://localhost:${server.info.port}/simple/single.m3u8`, { withData: true });
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
            }

            expect(checksums).to.equal(internals.checksums);
        });

        it('does not internally buffer (highWaterMark=0)', async () => {

            const reader = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'long.m3u8'));
            const streamer = new HlsSegmentStreamer(reader, { withData: false, highWaterMark: 0 });

            for await (const obj of streamer) {
                expect(obj).to.exist();
                await Hoek.wait(20);
                expect(streamer.readableLength).to.equal(0);
            }
        });

        it('supports the highWaterMark option', async () => {

            const r = createSimpleReader('file://' + Path.join(__dirname, 'fixtures', 'long.m3u8'), { highWaterMark: 2 });
            const buffered = [];

            for await (const obj of r) {
                expect(obj).to.exist();
                await Hoek.wait(20);
                buffered.push(r.readableLength);
            }

            expect(buffered).to.equal([2, 2, 2, 2, 1, 0]);
        });

        it('abort() also aborts active streams when withData is set', async () => {

            const r = createSimpleReader(`http://localhost:${server.info.port}/simple/slow.m3u8`, { withData: true, highWaterMark: 2 });
            const segments = [];

            setTimeout(() => r.abort(), 50);

            for await (const obj of r) {
                expect(obj.segment.msn).to.equal(segments.length);
                segments.push(obj);
            }

            expect(segments.length).to.equal(2);
        });

        it('abort() graceful is respected', async () => {

            const r = createSimpleReader(`http://localhost:${server.info.port}/simple/slow.m3u8`, { withData: true, stopDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });
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
        });

        it('can be destroyed', async () => {

            const r = createSimpleReader('file://' + Path.join(__dirname, 'fixtures', '500.m3u8'));
            const segments = [];

            for await (const obj of r) {
                segments.push(obj);
                r.destroy();
            }

            expect(segments.length).to.equal(1);
        });

        // handles all kinds of segment reference url
        // handles .m3u files
    });

    describe('live index', { parallel: false }, () => {

        const serverState = { state: {} };
        let liveServer;

        const prepareLiveReader = function (readerOptions = {}, state = {}) {

            const reader = createSimpleReader(`http://localhost:${liveServer.info.port}/live/live.m3u8`, { fullStream: false, withData: true, ...readerOptions });
            reader._intervals = [];
            reader._getUpdateInterval = function (updated) {

                this._intervals.push(HlsSegmentReader.prototype._getUpdateInterval.call(this, updated));
                return undefined;
            };

            serverState.state = { firstSeqNo: 0, segmentCount: 10, targetDuration: 2, ...state };

            return { reader, state: serverState.state };
        };

        beforeEach(() => {

            liveServer = Shared.provisionLiveServer(serverState);
            return liveServer.start();
        });

        afterEach(() => {

            return liveServer.stop();
        });

        it('handles a basic stream', async () => {

            const { reader, state } = prepareLiveReader({ fullStream: true });
            const segments = [];

            for await (const obj of reader) {
                expect(obj.segment.msn).to.equal(segments.length);
                segments.push(obj);

                if (obj.segment.msn > 5) {
                    state.firstSeqNo++;
                    if (state.firstSeqNo === 5) {
                        state.ended = true;
                        await Hoek.wait(50);
                    }
                }
            }

            expect(segments.length).to.equal(15);
        });

        it('handles sequence number resets', async () => {

            let reset = false;
            const { reader, state } = prepareLiveReader({ fullStream: false }, { firstSeqNo: 10, async index() {

                const index = Shared.genIndex(state);

                if (!reset) {
                    state.firstSeqNo++;

                    if (state.firstSeqNo === 16) {
                        state.firstSeqNo = 0;
                        state.segmentCount = 1;
                        reset = true;
                    }

                    await Hoek.wait(20);    // give the reader a chance to catch up
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
            expect(segments[0].segment.msn).to.equal(16);
            expect(segments[9].segment.msn).to.equal(0);
            expect(segments[8].segment.entry.discontinuity).to.be.false();
            expect(segments[9].segment.entry.discontinuity).to.be.true();
            expect(segments[10].segment.entry.discontinuity).to.be.false();
        });

        it('handles sequence number jumps', async () => {

            const { reader, state } = prepareLiveReader();
            const segments = [];
            let skipped = false;

            for await (const obj of reader) {
                segments.push(obj);

                if (!skipped && obj.segment.msn >= state.segmentCount - 1) {
                    state.firstSeqNo++;
                    if (state.firstSeqNo === 5) {
                        state.firstSeqNo = 50;
                        skipped = true;
                    }
                }

                if (skipped && obj.segment.msn > 55) {
                    state.firstSeqNo++;
                    if (state.firstSeqNo === 55) {
                        state.ended = true;
                        await Hoek.wait(50);
                    }
                }
            }

            expect(segments.length).to.equal(23);
            expect(segments[7].segment.msn).to.equal(13);
            expect(segments[7].segment.entry.discontinuity).to.be.false();
            expect(segments[8].segment.msn).to.equal(50);
            expect(segments[8].segment.entry.discontinuity).to.be.true();
            expect(segments[9].segment.entry.discontinuity).to.be.false();
        });

        // TODO: test problem emit & data outage
/*        it('handles a temporary server outage', async () => {

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

                    const index = Shared.genIndex(state);

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

            expect(segments.length).to.equal(30);
            expect(errors.length).to.be.greaterThan(0);
            expect(errors[0]).to.be.an.error('Internal Server Error');
        });*/

        it('aborts downloads that have been evicted from index', async () => {

            // Note: the eviction logic works on index updates, with a delay to allow an initial segment load some time to complete - otherwise it could be scheduled, have an immediate update, and be aborted before being given a chance

            const { reader, state } = prepareLiveReader({ fullStream: true }, { slow: true, targetDuration: 0 });
            const segments = [];

            for await (const obj of reader) {
                segments.push(obj);

                state.firstSeqNo++;
                if (state.firstSeqNo === 3) {
                    state.ended = true;
                }

                await Hoek.wait(50);
            }

            expect(segments.length).to.equal(13);
            expect(segments[0].stream.destroyed).to.be.true();

            reader.abort();
        });

        describe('with lowLatency=true', () => {

            const prepareLlReader = function (readerOptions = {}, state = {}, indexGen) {

                return prepareLiveReader({
                    lowLatency: true,
                    withData: true,
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

            const expectedBytes = function (segment) {

                if (segment.entry.parts) {
                    return segment.entry.parts.reduce((val, part, idx) => val + (1000 + segment.msn) + 100 * idx, 0);
                }

                return 5000 + segment.msn;
            };

            it('handles a basic low-latency stream', async () => {

                const { reader, state } = prepareLlReader({}, { partIndex: 4, end: { msn: 20, part: 3 } }, (query) => genLlIndex(query, state));

                const segments = [];
                for await (const obj of reader) {
                    expect(obj.segment.msn).to.equal(segments.length + 10);
                    segments.push(obj);

                    let bytes = 0;
                    for await (const chunk of obj.stream) {
                        bytes += chunk.length;
                    }

                    expect(bytes).to.equal(expectedBytes(obj.segment));
                }

                expect(segments.length).to.equal(11);
                expect(segments[0].segment.entry.parts).to.have.length(5);
            });

            it('handles a basic low-latency stream with initial full segment', async () => {

                const { reader, state } = prepareLlReader({}, { partIndex: 1, end: { msn: 20, part: 3 } }, (query) => genLlIndex(query, state));

                const segments = [];
                for await (const obj of reader) {
                    expect(obj.segment.msn).to.equal(segments.length + 9);
                    segments.push(obj);

                    let bytes = 0;
                    for await (const chunk of obj.stream) {
                        bytes += chunk.length;
                    }

                    expect(bytes).to.equal(expectedBytes(obj.segment));
                }

                expect(segments.length).to.equal(12);
                expect(segments[0].segment.details.parts).to.not.exist();
            });

            it('handles a basic low-latency stream using byteranges', async () => {

                const { reader, state } = prepareLlReader({}, { partIndex: 4, end: { msn: 20, part: 3 } }, (query) => {

                    const index = genLlIndex(query, state);
                    const firstMsn = index.first_seq_no;
                    for (let msn = firstMsn; msn <= index.lastSeqNo(); ++msn) {     // eslint-disable-line @hapi/hapi/for-loop
                        const segment = index.getSegment(msn);
                        if (segment.parts) {
                            for (let j = 0; j < segment.parts.length; ++j) {
                                const part = segment.parts[j];
                                part.set('uri', `${msn}.ts`, 'string');
                                part.set('byterange', { length: 800 + j, offset: j === 0 ? 0 : undefined }, 'byterange');
                            }
                        }
                    }

                    return index;
                });

                const segments = [];
                for await (const obj of reader) {
                    expect(obj.segment.msn).to.equal(segments.length + 10);
                    segments.push(obj);

                    let bytes = 0;
                    for await (const chunk of obj.stream) {
                        bytes += chunk.length;
                    }

                    // TODO:
//                    expect(bytes).to.equal(expectedBytes(obj.segment));

                    // TODO: validate range request was performed
                }

                expect(segments.length).to.equal(11);
                expect(segments[0].segment.details.parts).to.have.length(5);
            });

            // TODO: mp4 with initial map
            // TODO: part jumps
            // TODO: segment jumps
            // TODO: out of index stall

/*            it('handles a ll stream', async () => {

                const { reader, state } = prepareLlReader({ lowLatency: false }, { partIndex: 4, end: { msn: 20, part: 3 } }, (query) => genLlIndex(query, state));
                const segments = [];

                for await (const obj of reader) {
                    expect(obj.segment.msn).to.equal(segments.length);
                    segments.push(obj);

                    let bytes = 0;
                    for await (const chunk of obj.stream) {
                        bytes += chunk.length;
                    }

                    expect(bytes).to.equal(expectedBytes(obj.segment));
                }

                console.log('!!!', segments.map((obj) => obj.segment))
                expect(segments.length).to.equal(13);
            });*/
        });

        // handles fullStream option
        // emits index updates
        // TODO: resilience??
    });
});
