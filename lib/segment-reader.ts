import { assert as hoekAssert } from '@hapi/hoek';
import { MediaPlaylist, MasterPlaylist, MediaSegment, IndependentSegment, AttrList } from 'm3u8parse';
import { Readable } from 'readable-stream';

import { Deferred } from './helpers';
import { TypedReadable, ReadableEvents, TypedEmitter } from './raw/typed-readable';
import { HlsPlaylistReader, HlsPlaylistReaderOptions, ParsedPlaylist } from './playlist-reader';


// eslint-disable-next-line func-style
function assert(condition: any, ...args: any[]): asserts condition {

    hoekAssert(condition, ...args);
}


class SegmentPointer {

    msn: number;
    part?: number;

    constructor(msn = -1, part?: number) {

        this.msn = +msn;
        this.part = part;
    }

    next(): SegmentPointer {

        return new SegmentPointer(this.msn + 1, this.part === undefined ? undefined : 0);
    }

    isEmpty(): boolean {

        return this.msn < 0;
    }
}

export class HlsReaderObject {

    readonly msn: number;
    readonly isClosed: boolean;

    onUpdate?: ((entry: IndependentSegment, old?: IndependentSegment) => void) = undefined;

    private _entry: IndependentSegment;
    #closed?: Deferred<true>;

    constructor(msn: number, segment: IndependentSegment) {

        this.msn = msn;
        this._entry = new MediaSegment(segment) as IndependentSegment;
        this.isClosed = !segment.isPartial();
    }

    get entry(): IndependentSegment {

        return this._entry;
    }

    set entry(entry: IndependentSegment) {

        assert(!this.isClosed);

        const old = this._entry;
        this._entry = new MediaSegment(entry) as IndependentSegment;

        this._entry.discontinuity = !!(+entry.discontinuity | +old.discontinuity);

        this._update(!entry.isPartial(), old);
    }

    closed(): PromiseLike<true> | true {

        if (this.isClosed) {
            return true;
        }

        if (!this.#closed) {
            this.#closed = new Deferred<true>();
        }

        return this.#closed.promise;
    }

    abandon(): void {

        if (!this.isClosed) {
            return this._update(true);
        }
    }

    private _update(closed: boolean, old?: IndependentSegment): void {

        if (closed) {
            (<{ isClosed: boolean }> this).isClosed = true;
            if (this.#closed) {
                this.#closed.resolve(true);
            }
        }

        if (this.onUpdate) {
            process.nextTick(this.onUpdate.bind(this, this._entry, old));
        }
    }
}

export type HlsSegmentReaderOptions = {
    /** Start from first segment, or use stream start */
    fullStream?: boolean;

    /** Initial segment ends after this date */
    startDate?: Date | string | number;

    /** End when segment start after this date */
    stopDate?: Date | string | number;
} & HlsPlaylistReaderOptions;


const HlsReaderObjectType = <HlsReaderObject>(null as any);
const HlsSegmentReaderEvents = <IHlsSegmentReaderEvents & ReadableEvents<HlsReaderObject>>(null as any);
interface IHlsSegmentReaderEvents {
    problem(err: Error): void;
}

/**
 * Reads an HLS media playlist, and output segments in order.
 * Live & Event playlists are refreshed as needed, and expired segments are dropped when backpressure is applied.
 */
export class HlsSegmentReader extends TypedReadable(HlsReaderObjectType, TypedEmitter(HlsSegmentReaderEvents, Readable)) {

    readonly fullStream: boolean;
    startDate?: Date;
    stopDate?: Date; // TODO: move to reader??

    readonly reader: HlsPlaylistReader;

    #next = new SegmentPointer();
    #discont = false;
    #current: HlsReaderObject | null = null;

    constructor(src: string, options: HlsSegmentReaderOptions = {}) {

        super({ objectMode: true, highWaterMark: 0 });

        this.fullStream = !!options.fullStream;

        // Dates are inclusive

        this.startDate = options.startDate ? new Date(options.startDate) : undefined;
        this.stopDate = options.stopDate ? new Date(options.stopDate) : undefined;

        this.reader = new HlsPlaylistReader(src, options);

        this.reader.on<'error'>('error', this.destroy.bind(this));
        this.reader.on<'problem'>('problem', (err) => !this.destroyed && this.emit<'problem'>('problem', err));

        this.reader.on<'playlist'>('playlist', (playlist) => {

            // Update current entry with latest data

            if (this.#current && !this.#current.isClosed) {
                const currentSegment = playlist.index.getSegment(this.#current.msn, true);
                if (currentSegment && (currentSegment.isPartial() || currentSegment.parts)) {
                    this.#current.entry = currentSegment;
                }
            }
        });
    }

    get index(): Readonly<MediaPlaylist | MasterPlaylist> | undefined {

        return this.reader.index;
    }

    get hints(): ParsedPlaylist['preloadHints'] {

        return this.reader.hints;
    }

    /**
     * Called to push the next segment.
     */
    /*protected*/ async _read(): Promise<void> {

        try {
            let ready = true;
            while (ready) {
                try {
                    const last = this.#current;
                    this.#current = await this._getNextSegment();
                    last?.abandon();
                    ready = this.push(this.#current);
                }
                catch (err) {
                    if (this.index) {
                        if (this.index.master) {
                            this.push(null);                    // Just ignore any error
                            return;
                        }

                        if (this.reader.isRecoverableUpdateError(err)) {
                            continue;
                        }
                    }

                    throw err;
                }
            }
        }
        catch (err) {
            this.destroy(err);
        }
    }

    _destroy(err: Error | null, cb: unknown): void {

        super._destroy(err, cb as any);

        this.reader.destroy();
    }

    // Override to only destroy once

    destroy(err?: Error): this {

        if (!this.destroyed) {
            return super.destroy(err);
        }

        return this;
    }

    // Private methods

    private async _getNextSegment(): Promise<HlsReaderObject | null> {

        let playlist: ParsedPlaylist | undefined;
        while (playlist = await this.reader.waitForUpdate(playlist?.index)) {
            if (this.#next.isEmpty()) {
                this.#next = this._initialSegmentPointer(playlist);
            }
            else if ((this.#next.msn < playlist.index.startMsn(true)) ||
                     (this.#next.msn > (playlist.index.lastMsn(this.reader.lowLatency) + 1))) {

                // Playlist jump

                if (playlist.index.type /* VOD or event */) {
                    throw new Error('Fatal playlist inconsistency');
                }

                this.emit<'problem'>('problem', new Error('Playlist jump'));

                this.#next = new SegmentPointer(playlist.index.startMsn(true), this.reader.lowLatency ? 0 : undefined);
                this.#discont = true;
            }

            const next = this.#next;
            const segment = playlist.index.getSegment(next.msn, true);
            if (!segment ||
                (next.part === undefined && segment.isPartial()) ||
                (next.part && next.part >= (segment?.parts?.length || 0))) {

                if (playlist.index.ended) {
                    return null;        // Done - nothing more to do
                }

                continue;               // Try again
            }

            // Check if we need to stop

            if (this.stopDate && (segment.program_time || 0) > this.stopDate) {
                this.reader.destroy();
                return null;
            }

            // Apply cross playlist discontinuity

            if (this.#discont) {
                segment.discontinuity = true;
                this.#discont = false;
            }

            // Advance next

            this.#next = next.next();

            return new HlsReaderObject(next.msn, segment);
        }

        return null;
    }

    private _initialSegmentPointer(playlist: ParsedPlaylist): SegmentPointer {

        if (!this.fullStream && this.startDate) {
            const msn = playlist.index.msnForDate(this.startDate, true);
            if (msn >= 0) {
                return new SegmentPointer(msn, this.reader.lowLatency ? 0 : undefined);
            }

            // no date information in index
        }

        if (this.reader.lowLatency && playlist.serverControl.partHoldBack) {
            let partHoldBack = playlist.serverControl.partHoldBack;
            let msn = playlist.index.lastMsn(true);
            let segment;
            while (segment = playlist.index.getSegment(msn)) {
                // TODO: use INDEPENDENT=YES information for better start point

                if (segment.uri) {
                    partHoldBack -= segment.duration || 0;
                }
                else {
                    assert(segment.parts);
                    partHoldBack -= segment.parts.reduce((duration, part) => duration + part.get('duration', AttrList.Types.Float), 0);
                }

                if (partHoldBack <= 0) {
                    break;
                }

                msn--;
            }

            return new SegmentPointer(msn, 0);
        }

        // TODO: use start offset, when present

        return new SegmentPointer(playlist.index.startMsn(this.fullStream), this.reader.lowLatency ? 0 : undefined);
    }
}
