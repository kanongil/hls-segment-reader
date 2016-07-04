'use strict';

const Crypto = require('crypto');
const Fs = require('fs');
const Path = require('path');
const Readable = require('stream').Readable;
const Code = require('code');
const Hapi = require('hapi');
const Inert = require('inert');
const Lab = require('lab');
const M3U8Parse = require('m3u8parse');

const HlsSegmentReader = require('../segment-reader');


// Declare internals

const internals = {
    checksums: ['c38d0718851a20be2edba13fc1643c1076826c62',
                '612991f34ae7cc19df5d595a2a4249b8f5d2d3f0',
                'bc600f4039aae412c4d978b3fd4d608ce4dec59a']
};


// Test shortcuts

const lab = exports.lab = Lab.script();
const describe = lab.describe;
const it = lab.it;
const expect = Code.expect;


describe('HlsSegmentReader()', () => {

    const provisionServer = () => {

        const server = new Hapi.Server({ debug: false });

        server.register(Inert, () => {});

        server.connection({ routes: { files: { relativeTo: Path.join(__dirname, 'fixtures') } } });

        const delay = (request, reply) => {

            setTimeout(() => {

                return reply(200);
            }, 200);
        };

        const slowServe = (request, reply) => {

            const slowStream = new Readable();
            slowStream._read = () => {};

            const path = Path.join(__dirname, 'fixtures', request.params.path);
            const buffer = Fs.readFileSync(path);
            slowStream.push(buffer.slice(0, 5000));
            setTimeout(() => {

                slowStream.push(buffer.slice(5000));
                slowStream.push(null);
            }, 200);

            reply(slowStream).type('video/mp2t');
        };

        server.route({ method: 'GET', path: '/simple/{path*}', handler: { directory: { path: '.' } } });
        server.route({ method: 'GET', path: '/slow/{path*}', handler: { directory: { path: '.' } }, config: { pre: [{ method: delay, assign: 'delay' }] } });
        server.route({ method: 'GET', path: '/slow-data/{path*}', handler: slowServe });
        server.route({ method: 'GET', path: '/error', handler: (request, reply) => {

            reply.code(500);
        } });

        return server;
    };

    let server;

    lab.before((done) => {

        server = provisionServer();
        server.start(done);
    });

    lab.after((done) => {

        server.stop(done);
    });

    describe('constructor', () => {

        it('creates valid objects', (done) => {

            const r1 = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.m3u8');
            const r2 = HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.m3u8');

            expect(r1).to.be.instanceOf(HlsSegmentReader);
            expect(r2).to.be.instanceOf(HlsSegmentReader);

            r1.abort();
            r2.abort();
            done();
        });

        it('throws on missing uri option', (done) => {

            const createObject = () => {

                return new HlsSegmentReader();
            };

            expect(createObject).to.throw();
            done();
        });

        it('throws on invalid uri option', (done) => {

            const createObject = () => {

                return new HlsSegmentReader('asdf://test');
            };

            expect(createObject).to.throw();
            done();
        });
    });

    it('emits error on missing remote host', (done) => {

        const r = new HlsSegmentReader('http://does.not.exist/simple/500.m3u8');

        r.on('error', (err) => {

            expect(err).to.be.instanceOf(Error);
            done();
        });
    });

    it('emits error for missing data', (done) => {

        const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/notfound');

        r.on('error', (err) => {

            expect(err).to.be.instanceOf(Error);
            expect(err.message).to.contain('Not Found');
            done();
        });
    });

    it('emits error for http error responses', (done) => {

        const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/error');

        r.on('error', (err) => {

            expect(err).to.be.instanceOf(Error);
            expect(err.message).to.contain('Internal Server Error');
            done();
        });
    });

    it('emits error on non-index responses', (done) => {

        const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.ts');

        r.on('error', (err) => {

            expect(err).to.be.instanceOf(Error);
            expect(err.message).to.contain('Invalid MIME type');
            done();
        });
    });

    it('emits error on unknown segment mime type', (done) => {

        const r1 = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'badtype.m3u8'));

        r1.resume();
        r1.on('error', (err1) => {

            expect(err1).to.be.instanceOf(Error);
            expect(err1.message).to.contain('Unsupported segment MIME type');

            const r2 = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'badtype-data.m3u8'));

            r2.resume();
            r2.on('error', (err2) => {

                expect(err2).to.be.instanceOf(Error);
                expect(err2.message).to.contain('Unsupported segment MIME type');
                done();
            });
        });
    });

    it('emits error on malformed index files', (done) => {

        const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/malformed.m3u8');

        r.on('error', (err) => {

            expect(err).to.be.instanceOf(Error);
            done();
        });
    });

    describe('master index', () => {

        it('does not output any segments', (done) => {

            const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/index.m3u8');

            const segments = [];
            r.on('data', (segment) => {

                segments.push(segment);
            });

            r.on('end', () => {

                expect(segments.length).to.equal(0);
                done();
            });
        });

        it('emits "index" event', (done) => {

            const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/index.m3u8');

            let remoteIndex;
            r.on('index', (index) => {

                remoteIndex = index;
            });

            r.on('end', () => {

                expect(remoteIndex).to.exist();
                expect(remoteIndex.master).to.be.true();
                expect(remoteIndex.variants[0].uri).to.exist();
                done();
            });

            r.resume();
        });
    });

    describe('on-demand index', () => {

        it('outputs all segments', (done) => {

            const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.m3u8');

            const segments = [];
            r.on('data', (segment) => {

                expect(segment.seq).to.equal(segments.length);
                segments.push(segment);
            });

            r.on('end', () => {

                expect(segments.length).to.equal(3);
                done();
            });
        });

        it('emits the "index" event before starting', (done) => {

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', '500.m3u8'));

            let hasSegment = false;
            r.on('data', () => {

                hasSegment = true;
            });

            r.on('index', (index) => {

                expect(index).to.exist();
                expect(hasSegment).to.be.false();
                done();
            });
        });

        it('handles byte-range (file)', (done) => {

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'single.m3u8'), { withData: true });

            const checksums = [];
            r.on('data', (segment) => {

                r.pause();

                const hasher = Crypto.createHash('sha1');
                hasher.setEncoding('hex');

                segment.stream.pipe(hasher);
                segment.stream.on('end', () => {

                    checksums.push(hasher.read());
                    r.resume();

                    if (checksums.length === 3) {
                        expect(checksums).to.equal(internals.checksums);
                        done();
                    }
                });
            });
        });

        it('handles byte-range (http)', (done) => {

            const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/single.m3u8', { withData: true });

            const checksums = [];
            r.on('data', (segment) => {

                r.pause();

                const hasher = Crypto.createHash('sha1');
                hasher.setEncoding('hex');

                segment.stream.pipe(hasher);
                segment.stream.on('end', () => {

                    checksums.push(hasher.read());
                    r.resume();

                    if (checksums.length === 3) {
                        expect(checksums).to.equal(internals.checksums);
                        done();
                    }
                });
            });
        });

        it('supports the startDate option', (done) => {

            const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.m3u8', { startDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });

            const segments = [];
            r.on('data', (segment) => {

                expect(segment.seq).to.equal(segments.length + 2);
                segments.push(segment);
            });

            r.on('end', () => {

                expect(segments.length).to.equal(1);
                done();
            });
        });

        it('supports the stopDate option', (done) => {

            const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.m3u8', { stopDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });

            const segments = [];
            r.on('data', (segment) => {

                expect(segment.seq).to.equal(segments.length);
                segments.push(segment);
            });

            r.on('end', () => {

                expect(segments.length).to.equal(2);
                done();
            });
        });

        it('applies the extensions option', (done) => {

            const extensions = {
                '#EXT-MY-HEADER': false,
                '#EXT-MY-SEGMENT-OK': true
            };

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', '500.m3u8'), { extensions: extensions });

            const segments = [];
            r.on('data', (segment) => {

                segments.push(segment);
            });

            r.on('end', () => {

                expect(r.index).to.exist();
                expect(r.index.vendor['#EXT-MY-HEADER']).to.equal('hello');
                expect(r.index.segments[1].vendor).to.contain('#EXT-MY-SEGMENT-OK');
                expect(segments.length).to.equal(3);
                expect(segments[1].details.vendor).to.contain('#EXT-MY-SEGMENT-OK');
                done();
            });
        });

        it('supports the highWaterMark option', (done) => {

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'long.m3u8'), { highWaterMark: 2 });

            const buffered = [];
            r.on('data', (segment) => {

                r.pause();
                setTimeout(() => {

                    buffered.push(r._readableState.buffer.length);
                    r.resume();

                    if (segment.seq === 5) {
                        expect(buffered).to.equal([2, 2, 2, 2, 1, 0]);
                        done();
                    }
                }, 20);
            });
        });

        it('abort() also aborts active streams when withData is set', (done) => {

            const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/slow.m3u8', { withData: true, highWaterMark: 2 });

            const segments = [];
            r.on('data', (segment) => {

                expect(segment.seq).to.equal(segments.length);
                segments.push(segment);
            });

            r.on('end', () => {

                expect(segments.length).to.equal(2);
                done();
            });

            setTimeout(() => {

                r.abort();
            }, 50);
        });

        it('abort() graceful is respected', (done) => {

            const r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/slow.m3u8', { withData: true, stopDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });

            const checksums = [];
            r.on('data', (segment) => {

                r.pause();

                const hasher = Crypto.createHash('sha1');
                hasher.setEncoding('hex');

                segment.stream.pipe(hasher);
                segment.stream.on('end', () => {

                    checksums.push(hasher.read());
                    r.resume();

                    if (checksums.length === 2) {
                        expect(checksums).to.equal(internals.checksums.slice(0, 2));
                        done();
                    }
                });

                if (segment.seq === 1) {
                    r.abort(true);
                }
            });
        });

        it('can be destroyed', (done) => {

            const r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', '500.m3u8'));

            const segments = [];
            r.on('data', (segment) => {

                segments.push(segment);
                r.destroy();
            });

            r.on('end', () => {

                expect(segments.length).to.equal(1);
                done();
            });
        });

        // handles all kinds of segment reference url
        // handles highWaterMark option
        // handles .m3u files
    });

    describe('live index', { parallel: false }, () => {

        let state = {};
        let liveServer;

        lab.before((done) => {

            liveServer = new Hapi.Server({ debug: false });
            liveServer.connection({ routes: { files: { relativeTo: Path.join(__dirname, 'fixtures') } } });

            const serveIndex = (request, reply) => {

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
                    segments: segments,
                    ended: state.ended
                });

                reply(index.toString()).type('application/vnd.apple.mpegURL');
            };

            const serveSegment = (request, reply) => {

                if (state.slow) {
                    const slowStream = new Readable();
                    slowStream._read = () => {};

                    slowStream.push(new Buffer(5000));

                    return reply(slowStream).type('video/mp2t').bytes(30000);
                }

                const size = 5000 + parseInt(request.params.segment);
                return reply(new Buffer(size)).type('video/mp2t').bytes(size);
            };

            liveServer.route({ method: 'GET', path: '/live/live.m3u8', handler: serveIndex });
            liveServer.route({ method: 'GET', path: '/live/{segment}.ts', handler: serveSegment });

            liveServer.start(done);
        });

        lab.after((done) => {

            liveServer.stop(done);
        });

        lab.beforeEach((done) => {

            state = { firstSeqNo: 0, segmentCount: 10 };
            done();
        });

        it('handles a basic stream', (done) => {

            const r = new HlsSegmentReader('http://localhost:' + liveServer.info.port + '/live/live.m3u8', { fullStream: true });

            const segments = [];
            r.on('data', (segment) => {

                expect(segment.seq).to.equal(segments.length);
                segments.push(segment);

                if (segment.seq > 5) {
                    state.firstSeqNo++;
                    if (state.firstSeqNo === 5) {
                        state.ended = true;
                        r.pause();
                        setTimeout(() => {

                            r.resume();
                        }, 50);
                    }
                }
            });

            r.on('end', () => {

                expect(segments.length).to.equal(15);
                done();
            });
        });

        it('handles sequence number resets', (done) => {

            const r = new HlsSegmentReader('http://localhost:' + liveServer.info.port + '/live/live.m3u8', { fullStream: true });

            let reset = false;
            state.firstSeqNo = 10;

            const segments = [];
            r.on('data', (segment) => {

                segments.push(segment);

                if (!reset) {
                    state.firstSeqNo++;

                    if (state.firstSeqNo === 15) {
                        state.firstSeqNo = 0;
                        state.segmentCount = 1;
                        reset = true;

                        r.pause();
                        setTimeout(() => {

                            r.resume();
                        }, 50);
                    }
                }
                else {
                    state.segmentCount++;
                    if (state.segmentCount === 5) {
                        state.ended = true;
                        r.pause();
                        setTimeout(() => {

                            r.resume();
                        }, 50);
                    }
                }
            });

            r.on('end', () => {

                expect(segments.length).to.equal(11);
                expect(segments[6].seq).to.equal(0);
                expect(segments[5].details.discontinuity).to.be.false();
                expect(segments[6].details.discontinuity).to.be.true();
                expect(segments[7].details.discontinuity).to.be.false();
                done();
            });
        });

        it('handles sequence number jumps', (done) => {

            const r = new HlsSegmentReader('http://localhost:' + liveServer.info.port + '/live/live.m3u8', { fullStream: true });

            let skipped = false;

            const segments = [];
            r.on('data', (segment) => {

                segments.push(segment);

                if (!skipped && segment.seq >= state.segmentCount - 1) {
                    state.firstSeqNo++;
                    if (state.firstSeqNo === 5) {
                        state.firstSeqNo = 50;
                        skipped = true;
                    }
                }
                if (skipped && segment.seq > 55) {
                    state.firstSeqNo++;
                    if (state.firstSeqNo === 55) {
                        state.ended = true;
                        r.pause();
                        setTimeout(() => {

                            r.resume();
                        }, 50);
                    }
                }
            });

            r.on('end', () => {

                expect(segments.length).to.equal(29);
                expect(segments[14].seq).to.equal(50);
                expect(segments[13].details.discontinuity).to.be.false();
                expect(segments[14].details.discontinuity).to.be.true();
                expect(segments[15].details.discontinuity).to.be.false();
                done();
            });
        });

        it('aborts downloads that have been evicted from index', (done) => {

            const r = new HlsSegmentReader('http://localhost:' + liveServer.info.port + '/live/live.m3u8', { withData: true, fullStream: true });

            state.slow = true;

            const segments = [];
            r.on('data', (segment) => {

                segments.push(segment);

                state.firstSeqNo++;
                state.ended = true;

                r.pause();
                setTimeout(() => {

                    r.resume();
                }, 50);
            });

            r.on('end', () => {

                expect(segments.length).to.equal(11);
                expect(segments[0].stream.closed).to.be.true();

                r.abort();
                done();
            });
        });

        it('respects the maxStallTime option', (done) => {

            const r = new HlsSegmentReader('http://localhost:' + liveServer.info.port + '/live/live.m3u8', { maxStallTime: 5, fullStream: true });

            state.segmentCount = 1;

            r.on('error', (err) => {

                expect(err).to.be.instanceOf(Error);
                expect(err.message).to.contain('Index update stalled');

                done();
            });
        });

        // handles fullStream option
        // emits index updates
        // TODO: resilience??
    });
});
