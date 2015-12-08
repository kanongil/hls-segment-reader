var Crypto = require('crypto');
var Fs = require('fs');
var Path = require('path');
var Readable = require('stream').Readable;
var Code = require('code');
var Hapi = require('hapi');
var Inert = require('inert');
var Lab = require('lab');
var M3U8Parse = require('m3u8parse');

var HlsSegmentReader = require('../segment-reader');


// Declare internals

var internals = {
    checksums: ['c38d0718851a20be2edba13fc1643c1076826c62',
                '612991f34ae7cc19df5d595a2a4249b8f5d2d3f0',
                'bc600f4039aae412c4d978b3fd4d608ce4dec59a']
};


// Test shortcuts

var lab = exports.lab = Lab.script();
var describe = lab.describe;
var it = lab.it;
var expect = Code.expect;


describe('HlsSegmentReader()', function () {

    var provisionServer = function () {

        var server = new Hapi.Server({ debug: false });

        server.register(Inert, function () {});

        server.connection({ routes: { files: { relativeTo: Path.join(__dirname, 'fixtures') } } });

        var delay = function (request, reply) {

            setTimeout(function () {

                return reply(200);
            }, 200);
        };

        var slowServe = function (request, reply) {

            var slowStream = new Readable();
            slowStream._read = function () {};

            var path = Path.join(__dirname, 'fixtures', request.params.path);
            var buffer = Fs.readFileSync(path);
            slowStream.push(buffer.slice(0, 5000));
            setTimeout(function () {

                slowStream.push(buffer.slice(5000));
                slowStream.push(null);
            }, 200);

            reply(slowStream).type('video/mp2t');
        };

        server.route({ method: 'GET', path: '/simple/{path*}', handler: { directory: { path: '.' } } });
        server.route({ method: 'GET', path: '/slow/{path*}', handler: { directory: { path: '.' } }, config: { pre: [{ method: delay, assign: 'delay' }] } });
        server.route({ method: 'GET', path: '/slow-data/{path*}', handler: slowServe });
        server.route({ method: 'GET', path: '/error', handler: function (request, reply) {

            reply.code(500);
        } });

        return server;
    };

    var server;

    lab.before(function (done) {

        server = provisionServer();
        server.start(done);
    });

    lab.after(function (done) {

        server.stop(done);
    });

    describe('constructor', function () {

        it('creates valid objects', function (done) {

            var r1 = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.m3u8');
            var r2 = HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.m3u8');

            expect(r1).to.be.instanceOf(HlsSegmentReader);
            expect(r2).to.be.instanceOf(HlsSegmentReader);

            r1.abort();
            r2.abort();
            done();
        });

        it('throws on missing uri option', function (done) {

            var createObject = function () {

                return new HlsSegmentReader();
            };

            expect(createObject).to.throw();
            done();
        });

        it('throws on invalid uri option', function (done) {

            var createObject = function () {

                return new HlsSegmentReader('asdf://test');
            };

            expect(createObject).to.throw();
            done();
        });
    });

    it('emits error on missing remote host', function (done) {

        var r = new HlsSegmentReader('http://does.not.exist/simple/500.m3u8');

        r.on('error', function (err) {

            expect(err).to.be.instanceOf(Error);
            done();
        });
    });

    it('emits error for missing data', function (done) {

        var r = new HlsSegmentReader('http://localhost:' + server.info.port + '/notfound');

        r.on('error', function (err) {

            expect(err).to.be.instanceOf(Error);
            expect(err.message).to.contain('Not Found');
            done();
        });
    });

    it('emits error for http error responses', function (done) {

        var r = new HlsSegmentReader('http://localhost:' + server.info.port + '/error');

        r.on('error', function (err) {

            expect(err).to.be.instanceOf(Error);
            expect(err.message).to.contain('Internal Server Error');
            done();
        });
    });

    it('emits error on non-index responses', function (done) {

        var r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.ts');

        r.on('error', function (err) {

            expect(err).to.be.instanceOf(Error);
            expect(err.message).to.contain('Invalid MIME type');
            done();
        });
    });

    it('emits error on unknown segment mime type', function (done) {

        var r1 = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'badtype.m3u8'));

        r1.resume();
        r1.on('error', function (err1) {

            expect(err1).to.be.instanceOf(Error);
            expect(err1.message).to.contain('Unsupported segment MIME type');

            var r2 = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'badtype-data.m3u8'));

            r2.resume();
            r2.on('error', function (err2) {

                expect(err2).to.be.instanceOf(Error);
                expect(err2.message).to.contain('Unsupported segment MIME type');
                done();
            });
        });
    });

    it('emits error on malformed index files', function (done) {

        var r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/malformed.m3u8');

        r.on('error', function (err) {

            expect(err).to.be.instanceOf(Error);
            done();
        });
    });

    describe('variant index', function () {

        it('does not output any segments', function (done) {

            var r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/index.m3u8');

            var segments = [];
            r.on('data', function (segment) {

                segments.push(segment);
            });

            r.on('end', function () {

                expect(segments.length).to.equal(0);
                done();
            });
        });

        it('emits "index" event', function (done) {

            var r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/index.m3u8');

            var remoteIndex;
            r.on('index', function (index) {

                remoteIndex = index;
            });

            r.on('end', function () {

                expect(remoteIndex).to.exist();
                expect(remoteIndex.variant).to.be.true();
                expect(remoteIndex.programs['1'][0].uri).to.exist();
                done();
            });

            r.resume();
        });
    });

    describe('on-demand index', function () {

        it('outputs all segments', function (done) {

            var r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.m3u8');

            var segments = [];
            r.on('data', function (segment) {

                expect(segment.seq).to.equal(segments.length);
                segments.push(segment);
            });

            r.on('end', function () {

                expect(segments.length).to.equal(3);
                done();
            });
        });

        it('emits the "index" event before starting', function (done) {

            var r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', '500.m3u8'));

            var hasSegment = false;
            r.on('data', function () {

                hasSegment = true;
            });

            r.on('index', function (index) {

                expect(index).to.exist();
                expect(hasSegment).to.be.false();
                done();
            });
        });

        it('handles byte-range (file)', function (done) {

            var r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'single.m3u8'), { withData: true });

            var checksums = [];
            r.on('data', function (segment) {

                r.pause();

                var hasher = Crypto.createHash('sha1');
                hasher.setEncoding('hex');

                segment.stream.pipe(hasher);
                segment.stream.on('end', function () {

                    checksums.push(hasher.read());
                    r.resume();

                    if (checksums.length === 3) {
                        expect(checksums).to.deep.equal(internals.checksums);
                        done();
                    }
                });
            });
        });

        it('handles byte-range (http)', function (done) {

            var r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/single.m3u8', { withData: true });

            var checksums = [];
            r.on('data', function (segment) {

                r.pause();

                var hasher = Crypto.createHash('sha1');
                hasher.setEncoding('hex');

                segment.stream.pipe(hasher);
                segment.stream.on('end', function () {

                    checksums.push(hasher.read());
                    r.resume();

                    if (checksums.length === 3) {
                        expect(checksums).to.deep.equal(internals.checksums);
                        done();
                    }
                });
            });
        });

        it('supports the startDate option', function (done) {

            var r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.m3u8', { startDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });

            var segments = [];
            r.on('data', function (segment) {

                expect(segment.seq).to.equal(segments.length + 2);
                segments.push(segment);
            });

            r.on('end', function () {

                expect(segments.length).to.equal(1);
                done();
            });
        });

        it('supports the stopDate option', function (done) {

            var r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/500.m3u8', { stopDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });

            var segments = [];
            r.on('data', function (segment) {

                expect(segment.seq).to.equal(segments.length);
                segments.push(segment);
            });

            r.on('end', function () {

                expect(segments.length).to.equal(2);
                done();
            });
        });

        it('applies the extensions option', function (done) {

            var extensions = {
                '#EXT-MY-HEADER': false,
                '#EXT-MY-SEGMENT-OK': true
            };

            var r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', '500.m3u8'), { extensions: extensions });

            var segments = [];
            r.on('data', function (segment) {

                segments.push(segment);
            });

            r.on('end', function () {

                expect(r.index).to.exist();
                expect(r.index.vendor['#EXT-MY-HEADER']).to.equal('hello');
                expect(r.index.segments[1].vendor).to.contain('#EXT-MY-SEGMENT-OK');
                expect(segments.length).to.equal(3);
                expect(segments[1].details.vendor).to.contain('#EXT-MY-SEGMENT-OK');
                done();
            });
        });

        it('supports the highWaterMark option', function (done) {

            var r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', 'long.m3u8'), { highWaterMark: 2 });

            var buffered = [];
            r.on('data', function (segment) {

                r.pause();
                setTimeout(function () {

                    buffered.push(r._readableState.buffer.length);
                    r.resume();

                    if (segment.seq === 5) {
                        expect(buffered).to.deep.equal([2, 2, 2, 2, 1, 0]);
                        done();
                    }
                }, 20);
            });
        });

        it('abort() also aborts active streams when withData is set', function (done) {

            var r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/slow.m3u8', { withData: true, highWaterMark: 2 });

            var segments = [];
            r.on('data', function (segment) {

                expect(segment.seq).to.equal(segments.length);
                segments.push(segment);
            });

            r.on('end', function () {

                expect(segments.length).to.equal(2);
                done();
            });

            setTimeout(function () {

                r.abort();
            }, 50);
        });

        it('abort() graceful is respected', function (done) {

            var r = new HlsSegmentReader('http://localhost:' + server.info.port + '/simple/slow.m3u8', { withData: true, stopDate: new Date('Fri Jan 07 2000 07:03:09 GMT+0100 (CET)') });

            var checksums = [];
            r.on('data', function (segment) {

                r.pause();

                var hasher = Crypto.createHash('sha1');
                hasher.setEncoding('hex');

                segment.stream.pipe(hasher);
                segment.stream.on('end', function () {

                    checksums.push(hasher.read());
                    r.resume();

                    if (checksums.length === 2) {
                        expect(checksums).to.deep.equal(internals.checksums.slice(0, 2));
                        done();
                    }
                });

                if (segment.seq === 1) {
                    r.abort(true);
                }
            });
        });

        it('can be destroyed', function (done) {

            var r = new HlsSegmentReader('file://' + Path.join(__dirname, 'fixtures', '500.m3u8'));

            var segments = [];
            r.on('data', function (segment) {

                segments.push(segment);
                r.destroy();
            });

            r.on('end', function () {

                expect(segments.length).to.equal(1);
                done();
            });
        });

        // handles all kinds of segment reference url
        // handles highWaterMark option
        // handles .m3u files
    });

    describe('live index', { parallel: false }, function () {

        var state = {};
        var liveServer;

        lab.before(function (done) {

            liveServer = new Hapi.Server({ debug: false });
            liveServer.connection({ routes: { files: { relativeTo: Path.join(__dirname, 'fixtures') } } });

            var serveIndex = function (request, reply) {

                var segments = new Array(state.segmentCount);
                for (var idx = 0; idx < segments.length; idx++) {
                    segments[idx] = {
                        duration: 1,
                        uri: '' + (state.firstSeqNo + idx) + '.ts',
                        title: ''
                    };
                }

                var index = new M3U8Parse.M3U8Playlist({
                    'first_seq_no': state.firstSeqNo,
                    'target_duration': 0,
                    segments: segments,
                    ended: state.ended
                });

                reply(index.toString()).type('application/vnd.apple.mpegURL');
            };

            var serveSegment = function (request, reply) {

                if (state.slow) {
                    var slowStream = new Readable();
                    slowStream._read = function () {};

                    slowStream.push(new Buffer(5000));

                    return reply(slowStream).type('video/mp2t').bytes(30000);
                }

                var size = 5000 + parseInt(request.params.segment);
                return reply(new Buffer(size)).type('video/mp2t').bytes(size);
            };

            liveServer.route({ method: 'GET', path: '/live/live.m3u8', handler: serveIndex });
            liveServer.route({ method: 'GET', path: '/live/{segment}.ts', handler: serveSegment });

            liveServer.start(done);
        });

        lab.after(function (done) {

            liveServer.stop(done);
        });

        lab.beforeEach(function (done) {

            state = { firstSeqNo: 0, segmentCount: 10 };
            done();
        });

        it('handles a basic stream', function (done) {

            var r = new HlsSegmentReader('http://localhost:' + liveServer.info.port + '/live/live.m3u8', { fullStream: true });

            var segments = [];
            r.on('data', function (segment) {

                expect(segment.seq).to.equal(segments.length);
                segments.push(segment);

                if (segment.seq > 5) {
                    state.firstSeqNo++;
                    if (state.firstSeqNo === 5) {
                        state.ended = true;
                        r.pause();
                        setTimeout(function () {

                            r.resume();
                        }, 50);
                    }
                }
            });

            r.on('end', function () {

                expect(segments.length).to.equal(15);
                done();
            });
        });

        it('handles sequence number resets', function (done) {

            var r = new HlsSegmentReader('http://localhost:' + liveServer.info.port + '/live/live.m3u8', { fullStream: true });

            var reset = false;
            state.firstSeqNo = 10;

            var segments = [];
            r.on('data', function (segment) {

                segments.push(segment);

                if (!reset) {
                    state.firstSeqNo++;

                    if (state.firstSeqNo === 15) {
                        state.firstSeqNo = 0;
                        state.segmentCount = 1;
                        reset = true;

                        r.pause();
                        setTimeout(function () {

                            r.resume();
                        }, 50);
                    }
                } else {
                    state.segmentCount++;
                    if (state.segmentCount === 5) {
                        state.ended = true;
                        r.pause();
                        setTimeout(function () {

                            r.resume();
                        }, 50);
                    }
                }
            });

            r.on('end', function () {

                expect(segments.length).to.equal(11);
                expect(segments[6].seq).to.equal(0);
                expect(segments[5].details.discontinuity).to.be.false();
                expect(segments[6].details.discontinuity).to.be.true();
                expect(segments[7].details.discontinuity).to.be.false();
                done();
            });
        });

        it('handles sequence number jumps', function (done) {

            var r = new HlsSegmentReader('http://localhost:' + liveServer.info.port + '/live/live.m3u8', { fullStream: true });

            var skipped = false;

            var segments = [];
            r.on('data', function (segment) {

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
                        setTimeout(function () {

                            r.resume();
                        }, 50);
                    }
                }
            });

            r.on('end', function () {

                expect(segments.length).to.equal(29);
                expect(segments[14].seq).to.equal(50);
                expect(segments[13].details.discontinuity).to.be.false();
                expect(segments[14].details.discontinuity).to.be.true();
                expect(segments[15].details.discontinuity).to.be.false();
                done();
            });
        });

        it('aborts downloads that have been evicted from index', function (done) {

            var r = new HlsSegmentReader('http://localhost:' + liveServer.info.port + '/live/live.m3u8', { withData: true, fullStream: true });

            state.slow = true;

            var segments = [];
            r.on('data', function (segment) {

                segments.push(segment);

                state.firstSeqNo++;
                state.ended = true;

                r.pause();
                setTimeout(function () {

                    r.resume();
                }, 50);
            });

            r.on('end', function () {

                expect(segments.length).to.equal(11);
                expect(segments[0].stream.closed).to.be.true();

                r.abort();
                done();
            });
        });

        it('respects the maxStallTime option', function (done) {

            var r = new HlsSegmentReader('http://localhost:' + liveServer.info.port + '/live/live.m3u8', { maxStallTime: 5, fullStream: true });

            state.segmentCount = 1;

            r.on('error', function (err) {

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
