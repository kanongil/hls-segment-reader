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
const M3U8Parse = require('m3u8parse');
const Uristream = require('uristream');

const HlsSegmentReader = require('..');


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
const { after, before, beforeEach, describe, it } = lab;
const { expect } = Code;


describe('HlsSegmentReader()', () => {

    const provisionServer = () => {

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
            slowStream._read = () => {};

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
        server.route({ method: 'GET', path: '/error', handler(request, h) {

            throw new Error('!!!');
        } });

        return server;
    };

    const readSegments = (...args) => {

        let r;
        const promise = new Promise((resolve, reject) => {

            r = new HlsSegmentReader(...args);
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

    let server;

    before(async () => {

        server = await provisionServer();
        return server.start();
    });

    after(() => {

        return server.stop();
    });

    describe('constructor', () => {

        it('creates a valid object', () => {

            const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.m3u8');

            expect(r).to.be.instanceOf(HlsSegmentReader);

            r.abort();
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

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'badtype.m3u8'));

            for await (const obj of r) {

                expect(obj).to.exist();
            }
        })()).to.reject(Error, /Unsupported segment MIME type/);

        await expect((async () => {

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'badtype-data.m3u8'), { withData: true });

            for await (const obj of r) {

                expect(obj).to.exist();
            }
        })()).to.reject(Error, /Unsupported segment MIME type/);
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
                expect(segments[i].segment.seq).to.equal(i);
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

        it('handles byte-range (file)', async () => {

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'single.m3u8'), { withData: true });
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

            const r = new HlsSegmentReader(`http://localhost:${server.info.port}/simple/single.m3u8`, { withData: true });
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

        it('supports the startDate option', async () => {

            const r = new HlsSegmentReader(`http://localhost:${server.info.port}/simple/500.m3u8`, { startDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });
            const segments = [];

            for await (const obj of r) {
                expect(obj.segment.seq).to.equal(segments.length + 2);
                segments.push(obj);
            }

            expect(segments.length).to.equal(1);
        });

        it('supports the stopDate option', async () => {

            const r = new HlsSegmentReader(`http://localhost:${server.info.port}/simple/500.m3u8`, { stopDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });
            const segments = [];

            for await (const obj of r) {
                expect(obj.segment.seq).to.equal(segments.length);
                segments.push(obj);
            }

            expect(segments.length).to.equal(2);
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
            expect(segments.length).to.equal(3);
            expect(segments[1].segment.details.vendor[0]).to.equal(['#EXT-MY-SEGMENT-OK', null]);
        });

        it('supports the highWaterMark option', async () => {

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'long.m3u8'), { highWaterMark: 2 });
            const buffered = [];

            for await (const obj of r) {
                expect(obj).to.exist();
                await Hoek.wait(20);
                buffered.push(r._readableState.buffer.length);
            }

            expect(buffered).to.equal([2, 2, 2, 2, 1, 0]);
        });

        it('abort() also aborts active streams when withData is set', async () => {

            const r = new HlsSegmentReader(`http://localhost:${server.info.port}/simple/slow.m3u8`, { withData: true, highWaterMark: 2 });
            const segments = [];

            setTimeout(() => r.abort(), 50);

            for await (const obj of r) {
                expect(obj.segment.seq).to.equal(segments.length);
                segments.push(obj);
            }

            expect(segments.length).to.equal(2);
        });

        it('abort() graceful is respected', async () => {

            const r = new HlsSegmentReader(`http://localhost:${server.info.port}/simple/slow.m3u8`, { withData: true, stopDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });
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

                if (obj.segment.seq === 1) {
                    r.abort(true);
                }
            }

            expect(checksums).to.equal(internals.checksums.slice(1, 3));
        });

        it('can be destroyed', async () => {

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', '500.m3u8'));
            const segments = [];

            for await (const obj of r) {
                segments.push(obj);
                r.destroy();
            }

            expect(segments.length).to.equal(1);
        });

        // handles all kinds of segment reference url
        // handles highWaterMark option
        // handles .m3u files
    });

    describe('live index', { parallel: false }, () => {

        let state = {};
        let liveServer;

        before(() => {

            liveServer = new Hapi.Server({
                routes: { files: { relativeTo: Path.join(__dirname, 'fixtures') } },
                debug: false
            });

            const serveIndex = (request, h) => {

                const segments = new Array(state.segmentCount);
                for (let i = 0; i < segments.length; ++i) {
                    segments[i] = {
                        duration: 1,
                        uri: '' + (state.firstSeqNo + i) + '.ts',
                        title: ''
                    };
                }

                const index = new M3U8Parse.M3U8Playlist({
                    'first_seq_no': state.firstSeqNo,
                    'target_duration': 0,
                    segments,
                    ended: state.ended
                });

                return h.response(index.toString()).type('application/vnd.apple.mpegURL');
            };

            const serveSegment = (request, h) => {

                if (state.slow) {
                    const slowStream = new Readable();
                    slowStream._read = () => {};

                    slowStream.push(Buffer.alloc(5000));

                    return h.response(slowStream).type('video/mp2t').bytes(30000);
                }

                const size = 5000 + parseInt(request.params.segment);
                return h.response(Buffer.alloc(size)).type('video/mp2t').bytes(size);
            };

            liveServer.route({ method: 'GET', path: '/live/live.m3u8', handler: serveIndex });
            liveServer.route({ method: 'GET', path: '/live/{segment}.ts', handler: serveSegment });

            return liveServer.start();
        });

        after(() => {

            return liveServer.stop();
        });

        beforeEach(() => {

            state = { firstSeqNo: 0, segmentCount: 10 };
        });

        it('handles a basic stream', async () => {

            const r = new HlsSegmentReader(`http://localhost:${liveServer.info.port}/live/live.m3u8`, { fullStream: true });
            const segments = [];

            for await (const obj of r) {
                expect(obj.segment.seq).to.equal(segments.length);
                segments.push(obj);

                if (obj.segment.seq > 5) {
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

            const r = new HlsSegmentReader(`http://localhost:${liveServer.info.port}/live/live.m3u8`, { fullStream: true });
            const segments = [];
            let reset = false;

            state.firstSeqNo = 10;

            for await (const obj of r) {
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

            expect(segments.length).to.equal(11);
            expect(segments[6].segment.seq).to.equal(0);
            expect(segments[5].segment.details.discontinuity).to.be.false();
            expect(segments[6].segment.details.discontinuity).to.be.true();
            expect(segments[7].segment.details.discontinuity).to.be.false();
        });

        it('handles sequence number jumps', async () => {

            const r = new HlsSegmentReader(`http://localhost:${liveServer.info.port}/live/live.m3u8`, { fullStream: true });
            const segments = [];
            let skipped = false;

            for await (const obj of r) {
                segments.push(obj);

                if (!skipped && obj.segment.seq >= state.segmentCount - 1) {
                    state.firstSeqNo++;
                    if (state.firstSeqNo === 5) {
                        state.firstSeqNo = 50;
                        skipped = true;
                    }
                }

                if (skipped && obj.segment.seq > 55) {
                    state.firstSeqNo++;
                    if (state.firstSeqNo === 55) {
                        state.ended = true;
                        await Hoek.wait(50);
                    }
                }
            }

            expect(segments.length).to.equal(29);
            expect(segments[14].segment.seq).to.equal(50);
            expect(segments[13].segment.details.discontinuity).to.be.false();
            expect(segments[14].segment.details.discontinuity).to.be.true();
            expect(segments[15].segment.details.discontinuity).to.be.false();
        });

        it('aborts downloads that have been evicted from index', async () => {

            const r = new HlsSegmentReader(`http://localhost:${liveServer.info.port}/live/live.m3u8`, { withData: true, fullStream: true });
            const segments = [];

            state.slow = true;

            for await (const obj of r) {
                segments.push(obj);

                state.firstSeqNo++;
                state.ended = true;

                await Hoek.wait(50);
            }

            expect(segments.length).to.equal(11);
            expect(segments[0].stream.closed).to.be.true();

            r.abort();
        });

        it('respects the maxStallTime option', async () => {

            const r = new HlsSegmentReader(`http://localhost:${liveServer.info.port}/live/live.m3u8`, { maxStallTime: 5, fullStream: true });

            state.segmentCount = 1;

            await expect((async () => {

                for await (const obj of r) {

                    expect(obj).to.exist();
                }
            })()).to.reject(Error, /Index update stalled/);
        });

        // handles fullStream option
        // emits index updates
        // TODO: resilience??
    });
});
