/* eslint-disable @typescript-eslint/no-loop-func */

import { once } from 'node:events';
import Net from 'node:net';
import { Readable } from 'node:stream';

import Boom from '@hapi/boom';
import { expect } from '@hapi/code';
import Hapi from '@hapi/hapi';
import Joi from 'joi';

import { assert, ContentFetcher as ContentFetcherDefault, Deferred, wait, webstreamImpl as WS } from 'hls-playlist-reader/helpers';
import { ContentFetcher as ContentFetcherWeb } from 'hls-playlist-reader/helpers.web';

import { PartStream as PartStreamNode } from '../lib/part-stream.node.js';
import { PartStream as PartStreamWeb } from '../lib/part-stream.web.js';


class InstrumentedSignal extends EventTarget implements AbortSignal {

    readonly aborted = false;
    readonly reason = undefined;

    onabort: ((this: AbortSignal, ev: Event) => any) | null = null;

    // eslint-disable-next-line @typescript-eslint/ban-types
    #listeners = new Set<unknown>();

    addEventListener(type: string, callback: EventListenerOrEventListenerObject | null, options?: boolean | AddEventListenerOptions | undefined): void {

        assert(type === 'abort', 'Only "abort" can be used');
        assert(!options || options === true || !options.signal, '"signal" option is not supported yet');

        /*if (options && typeof options !== 'boolean') {
            options.signal?.addEventListener('abort', );
        }*/

        if (callback) {
            this.#listeners.add(callback);
        }
    }

    removeEventListener(type: string, callback: EventListenerOrEventListenerObject | null, options?: boolean | EventListenerOptions | undefined): void {

        assert(type === 'abort', 'Only "abort" can be used');

        this.#listeners.delete(callback);
    }

    // eslint-disable-next-line @typescript-eslint/no-empty-function
    throwIfAborted() {}

    _finish(): number {

        const remaining = this.#listeners.size;
        this.#listeners.clear();
        return remaining;
    }
}

const devNull = async (stream?: ReadableStream | Readable): Promise<number> => {

    if (stream instanceof WS.ReadableStream) {
        return new Promise<number>((resolve, reject) => {

            let consumed = 0;
            stream.pipeTo(new WS.WritableStream<Uint8Array>({
                abort: reject,
                write(c) {

                    consumed += c.byteLength;
                },
                close: () => resolve(consumed)
            }));
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


const testMatrix = new Map(Object.entries({
    'default': { PartStream: PartStreamNode, ContentFetcher: ContentFetcherDefault },
    'web': { PartStream: PartStreamWeb, ContentFetcher: ContentFetcherWeb, skip: typeof fetch !== 'function' }
}));

for (const [label, { PartStream, ContentFetcher, skip }] of testMatrix) {

    describe(`PartStream (${label})`, () => {

        const intApi = {
            requests: [] as string[],
            head: {
                segment: -1,
                part: -1
            },
            waiting: undefined as Deferred<void> | undefined,
            errored: new Map<string, number>(),

            wait(segment: number, part: number): Promise<void> | true {

                const { head } = this;

                if (segment < head.segment ||
                    (segment === head.segment && part <= head.part)) {

                    return true;   // Nothing to do
                }

                return (this.waiting ??= new Deferred()).promise;
            },

            async advanceTo(segment: number, part = 0, withError?: Error): Promise<void> {

                assert(segment > this.head.segment || (segment === this.head.segment && part > this.head.part));

                await wait(40);     // Wait a while so client request has actually reached the server
                this.head = { segment, part };
                withError ? this.waiting?.reject(withError) : this.waiting?.resolve();
                this.waiting = undefined;
            },

            reset(): void {

                this.requests = [];
                this.head = {
                    segment: -1,
                    part: -1
                };

                this.waiting?.reject(new Error('reset'));
                this.waiting = undefined;
                this.errored = new Map();
            }
        };

        // eslint-disable-next-line no-spaced-func
        const server = new Hapi.Server<typeof intApi>({
            host: '127.0.0.1',
            routes: { files: { relativeTo: new URL('fixtures', import.meta.url).pathname } }
        });

        server.app = intApi;

        server.route({
            method: 'get',
            path: '/stream/s{segment}.{ext}',
            async handler(request, h) {

                let { hint, bytes, error, times } = request.query as { hint?: boolean; bytes: number; error?: number | string; times: number };
                const rid = `s${request.params.segment}${request.url.search}`;

                server.app.requests.push(rid);

                const [, segment, part/*, _ext*/] = /^(\d*)@(\d*)/.exec(request.params.segment)!;
                const ready = await server.app.wait(parseInt(segment, 10), parseInt(part, 10));
                if (hint && ready) {
                    throw Boom.teapot('No longer a hint');
                }

                if (error) {
                    const remaining = (server.app.errored.get(rid) ?? times) - 1;
                    server.app.errored.set(rid, remaining);
                    if (remaining < 0) {
                        error = undefined;
                    }
                }

                if (error) {
                    if (typeof error === 'number') {
                        throw new Boom.Boom('Fail', { statusCode: error });
                    }
                }

                const stream = new Readable({
                    // eslint-disable-next-line @typescript-eslint/no-empty-function
                    read() {}
                });

                stream.push(Buffer.alloc(500));
                if (error) {
                    setTimeout(() => request.raw.res.destroy(), 20);
                }
                else {
                    stream.push(Buffer.alloc(bytes - 500));
                    stream.push(null);
                }

                const response = h.response(stream).type('video/mp2t');
                if (ready === true) {
                    response.bytes(bytes);
                }

                return response;
            },
            options: {
                validate: {
                    params: Joi.object({
                        segment: Joi.string(),
                        ext: Joi.string()
                    }),
                    query: Joi.object({
                        hint: Joi.boolean().truthy('').default(false),
                        bytes: Joi.number().integer().default(10000),
                        error: Joi.alternatives(Joi.number().integer(), Joi.string()),
                        times: Joi.number().integer().default(1)
                    }).unknown(true)
                }
            }
        });

        const fetcher = new ContentFetcher();
        let signal: InstrumentedSignal;
        let baseUrl: string;

        before(async function () {

            if (skip) {
                return this.skip();
            }

            await server.start();
            baseUrl = `${server.info.uri}/stream/index.m3u8`;
        });

        after(() => server.stop());

        beforeEach(() => {

            server.app.reset();
            signal = new InstrumentedSignal();
        });

        afterEach(() => {

            const remaining = signal._finish();
            expect(remaining).to.equal(0);
        });

        it('can start with only a hint', async () => {

            const stream = new PartStream(fetcher as any, { baseUrl, signal });
            const promise = devNull(stream);

            stream.append(undefined, { part: { uri: 's0@0.ts?hint' } });

            await server.app.advanceTo(0, 0);
            stream.append([{ uri: 's0@0.ts?hint' }], { part: { uri: 's0@1.ts?hint' } });

            const meta = await stream.meta;
            expect(meta).to.contain({ mime: 'video/mp2t', size: -1 });

            await server.app.advanceTo(0, 1);
            stream.append([{ uri: 's0@1.ts?hint' }], { part: { uri: 's1@0.ts?hint' } }, true);

            const consumed = await promise;
            expect(consumed).to.equal(20000);

            expect(server.app.requests).to.equal([
                's0@0?hint',
                's0@1?hint'
            ]);
        });

        it('can start with multiple parts', async () => {

            await server.app.advanceTo(0, 1);

            const stream = new PartStream(fetcher as any, { baseUrl, signal });
            const promise = devNull(stream);

            stream.append([{ uri: 's0@0.ts' }, { uri: 's0@1.ts' }], { part: { uri: 's0@2.ts?hint' } });

            const meta = await stream.meta;
            expect(meta).to.contain({ mime: 'video/mp2t', size: -1 });

            await server.app.advanceTo(0, 2);
            stream.append([{ uri: 's0@2.ts?hint' }], { part: { uri: 's1@0.ts?hint' } }, true);

            const consumed = await promise;
            expect(consumed).to.equal(30000);

            expect(server.app.requests).to.equal([
                's0@0',
                's0@1',
                's0@2?hint'
            ]);
        });

        it('can start with a complete segment', async () => {

            await server.app.advanceTo(0, 1);

            const stream = new PartStream(fetcher as any, { baseUrl, signal });
            const promise = devNull(stream);

            stream.append([{ uri: 's0@0.ts' }, { uri: 's0@1.ts' }], { part: { uri: 's1@0.ts?hint' } }, true);

            const meta = await stream.meta;
            expect(meta).to.contain({ mime: 'video/mp2t', size: -1 });

            const consumed = await promise;
            expect(consumed).to.equal(20000);

            expect(server.app.requests).to.equal([
                's0@0',
                's0@1'
            ]);
        });

        it('does not pipeline multiple part requests', async () => {

            let clients = 0;
            const raw = Net.createServer((socket) => {

                ++clients;

                socket.resume();    // Ignore request

                socket.write('HTTP/1.1 200 OK\r\n', 'ascii');
                socket.write('Content-Type: video/mp2t\r\n', 'ascii');
                socket.write('Content-Length: 10000\r\n\r\n', 'ascii');
                socket.write(Buffer.alloc(10000));
                socket.end();
            }).listen();

            await once(raw, 'listening');

            try {
                const stream = new PartStream(fetcher as any, { baseUrl: `http://127.0.0.1:${(<any>raw.address()).port}/`, signal });
                const promise = devNull(stream);

                stream.append([{ uri: 's0@0.ts' }, { uri: 's0@1.ts' }], { part: { uri: 's1@0.ts?hint' } }, true);

                const meta = await stream.meta;
                expect(meta).to.contain({ mime: 'video/mp2t', size: -1 });

                const consumed = await promise;
                expect(consumed).to.equal(20000);

                expect(clients).to.equal(2);
            }
            finally {
                raw.close();
            }
        });

        it('handles an unused hint', async () => {

            await server.app.advanceTo(0, 0);

            const stream = new PartStream(fetcher as any, { baseUrl, signal });
            const promise = devNull(stream);

            stream.append([{ uri: 's0@0.ts' }], { part: { uri: 's0@1.ts?hint' } });

            const meta = await stream.meta;
            expect(meta).to.contain({ mime: 'video/mp2t', size: -1 });

            await server.app.advanceTo(0, 1);
            stream.append([{ uri: 's0@1.ts?bytes=5000' }], { part: { uri: 's1@0.ts?hint' } }, true);

            const consumed = await promise;
            expect(consumed).to.equal(15000);

            expect(server.app.requests).to.equal([
                's0@0',
                's0@1?hint',
                's0@1?bytes=5000'
            ]);
        });

        it('recovers from a hint fetch error (request)', async () => {

            await server.app.advanceTo(0, 0);

            const stream = new PartStream(fetcher as any, { baseUrl, signal });
            const promise = devNull(stream);

            stream.append([{ uri: 's0@0.ts' }], { part: { uri: 's0@1.ts?error=404&times=1' } });

            const meta = await stream.meta;
            expect(meta).to.contain({ mime: 'video/mp2t', size: -1 });

            await server.app.advanceTo(0, 1);
            stream.append([{ uri: 's0@1.ts?error=404&times=1' }], { part: { uri: 's1@0.ts?hint' } }, true);

            const consumed = await promise;
            expect(consumed).to.equal(20000);

            expect(server.app.requests).to.equal([
                's0@0',
                's0@1?error=404&times=1',
                's0@1?error=404&times=1'
            ]);
        });

        it('fails on multiple hint fetch errors (request)', async () => {

            await server.app.advanceTo(0, 0);

            const stream = new PartStream(fetcher as any, { baseUrl, signal });
            const promise = devNull(stream);

            stream.append([{ uri: 's0@0.ts' }], { part: { uri: 's0@1.ts?error=500&times=2' } });

            const meta = await stream.meta;
            expect(meta).to.contain({ mime: 'video/mp2t', size: -1 });

            await server.app.advanceTo(0, 1);
            stream.append([{ uri: 's0@1.ts?error=500&times=2' }], { part: { uri: 's1@0.ts?hint' } }, true);

            await expect(promise).to.reject(Error);

            expect(server.app.requests).to.equal([
                's0@0',
                's0@1?error=500&times=2',
                's0@1?error=500&times=2'
            ]);
        });

        it('recovers from a part fetch error (request)', async () => {

            await server.app.advanceTo(0, 1);

            const stream = new PartStream(fetcher as any, { baseUrl, signal });
            const promise = devNull(stream);

            stream.append([{ uri: 's0@0.ts' }, { uri: 's0@1.ts?error=404&times=1' }], { part: { uri: 's0@2.ts' } });

            const meta = await stream.meta;
            expect(meta).to.contain({ mime: 'video/mp2t', size: -1 });

            await server.app.advanceTo(0, 2);
            stream.append([{ uri: 's0@2.ts' }], { part: { uri: 's1@0.ts?hint' } }, true);

            const consumed = await promise;
            expect(consumed).to.equal(30000);

            expect(server.app.requests).to.have.length(5);

            const expected = [
                's0@0',
                's0@1?error=404&times=1',
                's0@2',                        // The pending request is cancelled
                's0@1?error=404&times=1',
                's0@2'
            ];

            if (server.app.requests[2] === 's0@1?error=404&times=1') {
                expected.splice(3, 0, ...expected.splice(2, 1));
            }

            expect(server.app.requests).to.equal(expected);
        });

        it('fails on a hint fetch error (data)', async () => {

            // TODO: retry on fetch data errors??

            await server.app.advanceTo(0, 0);

            const stream = new PartStream(fetcher as any, { baseUrl, signal });
            const promise = devNull(stream);

            stream.append([{ uri: 's0@0.ts' }], { part: { uri: 's0@1.ts?error=dc' } });

            const meta = await stream.meta;
            expect(meta).to.contain({ mime: 'video/mp2t', size: -1 });

            await server.app.advanceTo(0, 1);
            stream.append([{ uri: 's0@1.ts?error=dc' }], { part: { uri: 's1@0.ts?hint' } }, true);

            await expect(promise).to.reject(Error);

            expect(server.app.requests).to.equal([
                's0@0',
                's0@1?error=dc'
            ]);
        });

        it('upstream signal is made into an AbortError (pre-meta)', async () => {

            const ac = new AbortController();
            const stream = new PartStream(fetcher as any, { baseUrl, signal: ac.signal });
            const promise = devNull(stream);

            stream.append(undefined, { part: { uri: 's0@0.ts?hint' } });

            await wait(1);
            const err = new Error('fail');
            ac.abort(err);

            const metaErr = await expect(stream.meta).to.reject(Error);
            expect(metaErr).to.not.shallow.equal(err);
            expect(metaErr.name).to.equal('AbortError');

            const streamErr = await expect(promise).to.reject(Error);
            expect(streamErr).to.not.shallow.equal(err);
            expect(streamErr.name).to.equal('AbortError');
        });

        it('upstream signal is made into an AbortError (post-meta)', async () => {

            await server.app.advanceTo(0, 0);

            const ac = new AbortController();
            const stream = new PartStream(fetcher as any, { baseUrl, signal: ac.signal });
            const promise = devNull(stream);

            stream.append([{ uri: 's0@0.ts' }], { part: { uri: 's0@1.ts?hint' } });

            const meta = await stream.meta;
            expect(meta).to.contain({ mime: 'video/mp2t', size: -1 });

            const err = new Error('fail');
            ac.abort(err);

            const streamErr = await expect(promise).to.reject(Error);
            expect(streamErr).to.not.shallow.equal(err);
            expect(streamErr.name).to.equal('AbortError');
        });

        describe('abandon()', () => {

            it('works', async () => {

                const stream = new PartStream(fetcher as any, { baseUrl, signal });

                stream.append(undefined, { part: { uri: 's0@0.ts?hint' } });

                await wait(1);

                expect(() => stream.abandon()).to.not.throw();
            });

            it('works after meta errors', async () => {

                await server.app.advanceTo(0, 0);

                const stream = new PartStream(fetcher as any, { baseUrl, signal });

                stream.append([{ uri: 's0@0.ts?error=500&times=10' }], { part: { uri: 's0@1.ts?hint' } });

                await expect(stream.meta).to.reject(Error);

                expect(() => stream.abandon()).to.not.throw();
            });
        });
    });
}
