import type { HlsIndexMeta } from 'hls-playlist-reader';
import type { MediaPlaylist, MasterPlaylist } from 'm3u8parse';

import { ReadableEvents, TypedEmitter, TypedReadable } from 'hls-playlist-reader/lib/raw/typed-readable';

import { HlsFetcherObject, HlsSegmentFetcher, HlsSegmentFetcherOptions } from './segment-fetcher';


export type HlsSegmentReaderOptions = Omit<HlsSegmentFetcherOptions, 'onProblem'>;


const HlsSegmentReaderEvents = <IHlsSegmentReaderEvents & ReadableEvents<HlsFetcherObject>>(null as any);
interface IHlsSegmentReaderEvents {
    index(index: Readonly<MasterPlaylist | MediaPlaylist>, meta: Readonly<HlsIndexMeta>): void;
    problem(err: Error): void;
}

/**
 * Reads an HLS media playlist, and output segments in order.
 * Live & Event playlists are refreshed as needed, and expired segments are dropped when backpressure is applied.
 */
export class HlsSegmentReader extends TypedEmitter(HlsSegmentReaderEvents, TypedReadable<HlsFetcherObject>()) {

    readonly fetcher: HlsSegmentFetcher;
    readonly index?: Readonly<MediaPlaylist | MasterPlaylist>;

    constructor(src: string, options: HlsSegmentReaderOptions = {}) {

        super({ objectMode: true, highWaterMark: 0, autoDestroy: true, emitClose: true });

        this.fetcher = new HlsSegmentFetcher(src, {
            ...options,
            onProblem: (err) => !this.destroyed && this.emit<'problem'>('problem', err),
            onIndex: (index, meta) => {

                (<{ index: Readonly<MediaPlaylist | MasterPlaylist> }> this).index = index;
                this.emit('index', index, meta);
            }
        });

        this.fetcher.start().catch(this.destroy.bind(this));
    }

    /**
     * Called to push the next segment.
     */
    /*protected*/ async _read(): Promise<void> {

        try {
            const result = await this.fetcher.next();
            if (!result) {
                this.fetcher.cancel();
            }

            this.push(result);
        }
        catch (err) {
            if (this.index?.master === true) {
                this.push(null);                    // Just ignore any error
                this.fetcher.cancel();
                return;
            }

            this.destroy(err instanceof Error ? err : new Error('Unknown exception'));
        }
    }

    _destroy(err: Error | null, cb: unknown): void {

        if (err?.name === 'AbortError') {
            err = null;
        }

        this.fetcher.cancel(err ?? undefined);

        super._destroy(err, cb as any);
    }
}
