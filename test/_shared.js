'use strict';

const Fs = require('fs');
const Path = require('path');
const Readable = require('stream').Readable;

const Hapi = require('@hapi/hapi');
const Hoek = require('@hapi/hoek');
const Inert = require('@hapi/inert');
const Joi = require('joi');
const M3U8Parse = require('m3u8parse');


exports.provisionServer = () => {

    const server = new Hapi.Server({
        host: '127.0.0.1',
        routes: { files: { relativeTo: Path.join(__dirname, 'fixtures') } }
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

        return h.response(slowStream).type('video/mp2t').header('content-length', buffer.byteLength);
    };

    server.route({ method: 'GET', path: '/simple/{path*}', handler: { directory: { path: '.' } } });
    server.route({ method: 'GET', path: '/slow/{path*}', handler: { directory: { path: '.' } }, config: { pre: [{ method: delay, assign: 'delay' }] } });
    server.route({ method: 'GET', path: '/slow-data/{path*}', handler: slowServe });
    server.route({
        method: 'GET', path: '/error', handler(request, h) {

            throw new Error('!!!');
        }
    });

    server.ext('onRequest', (request, h) => {

        if (server.onRequest) {
            server.onRequest(request);
        }

        return h.continue;
    });

    return server;
};

exports.provisionLiveServer = function (shared) {

    const server = new Hapi.Server({
        routes: {
            files: { relativeTo: Path.join(__dirname, 'fixtures') }
        }
    });

    const serveLiveIndex = async (request, h) => {

        let index;
        if (shared.state.index) {
            index = await shared.state.index(request.query);
        }
        else {
            index = exports.genIndex(shared.state);
        }

        return h.response(index.toString()).type('application/vnd.apple.mpegURL');
    };

    const serveSegment = (request, h) => {

        if (shared.state.slow) {
            const slowStream = new Readable({ read: Hoek.ignore });

            slowStream.push(Buffer.alloc(5000));

            return h.response(slowStream).type('video/mp2t').bytes(30000);
        }

        const size = ~~(5000 / (request.params.part === undefined ? 1 : shared.state.partCount)) + parseInt(request.params.msn) + 100 * parseInt(request.params.part || 0);

        if (shared.state.unstable) {
            --shared.state.unstable;

            const unstableStream = new Readable({ read: Hoek.ignore });

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


exports.readSegments = async (Class, ...args) => {

    /** @type { ReadableStream<unknown> } */
    const r = new Class(...args);
    const reader = (r.readable ?? r).getReader();
    const segments = [];

    for (;;) {
        const { done, value } = await reader.read();
        if (done) {
            return segments;
        }

        segments.push(value);
    }
};


exports.genIndex = function ({ targetDuration, segmentCount, firstMsn, partCount, partIndex, ended }) {

    const partDuration = targetDuration / partCount;

    const segments = [];
    const meta = {};

    for (let i = 0; i < segmentCount; ++i) {
        const parts = [];
        if (i >= segmentCount - 2) {
            for (let j = 0; j < partCount; ++j) {
                parts.push(new M3U8Parse.AttrList({
                    duration: partDuration,
                    uri: `"${firstMsn + i}-part${j}.ts"`
                }));
            }
        }

        segments.push({
            duration: targetDuration || 2,
            uri: `${firstMsn + i}.ts`,
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
                    uri: `"${firstMsn + segmentCount}-part${i}.ts"`
                }));
            }

            segments.push({ parts });
        }

        // Add hint

        if (!ended) {
            meta.preload_hints = [new M3U8Parse.AttrList({
                type: 'part',
                uri: `"${firstMsn + segmentCount}-part${partIndex}.ts"`
            })];
        }
    }

    const index = new M3U8Parse.MediaPlaylist({
        media_sequence: firstMsn,
        target_duration: targetDuration,
        part_info: partCount ? new M3U8Parse.AttrList({ 'part-target': partDuration }) : undefined,
        segments,
        meta,
        ended
    });

    //console.log('GEN', index.startMsn(true), index.lastMsn(true), index.meta.preload_hints, index.ended);

    return index;
};


exports.genLlIndex = function (query, state) {

    // Return playlist with exactly the next part

    if (!state.ended && query._HLS_msn !== undefined) {
        let msn = query._HLS_msn;
        let part = query._HLS_part === undefined ? state.partCount : query._HLS_part + 1;

        if (part >= state.partCount) {
            msn++;
            part = 0;
        }

        state.firstMsn = msn - state.segmentCount;
        state.partIndex = part;
    }

    const index = exports.genIndex(state);

    index.server_control = new M3U8Parse.AttrList({
        'can-block-reload': 'YES',
        'part-hold-back': 3 * state.targetDuration / state.partCount
    });

    state.genCount = (state.genCount || 0) + 1;

    if (!state.ended) {
        if (state.end &&
            index.lastMsn() > state.end.msn || (index.lastMsn() === state.end.msn && state.end.part === index.getSegment(index.lastMsn()).parts.length)) {

            index.ended = state.ended = true;
            delete index.meta.preload_hints;
            return index;
        }

        state.partIndex = ~~state.partIndex + 1;
        if (state.partIndex >= state.partCount) {
            state.partIndex = 0;
            state.firstMsn++;
        }
    }

    return index;
};
