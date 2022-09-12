import { HlsPlaylistFetcher, HlsPlaylistFetcherOptions } from 'hls-playlist-reader/fetcher';
import { M3U8Playlist, MediaPlaylist } from 'm3u8parse';
import { HlsFetcherObject, HlsSegmentFetcher, HlsSegmentFetcherOptions } from './segment-fetcher.js';
import { HlsSegmentReadable } from './segment-readable.js';
import { HlsSegmentStreamer, HlsSegmentStreamerOptions } from './segment-streamer.js';

export { HlsFetcherObject } from './segment-fetcher.js';
export type { HlsIndexMeta } from 'hls-playlist-reader';
export { HlsStreamerObject } from './segment-streamer.js';

class UnendingPlaylistFetcher extends HlsPlaylistFetcher {

    protected preprocessIndex<T extends M3U8Playlist>(index: T): T | undefined {

        if (!index.master) {
            MediaPlaylist.cast(index).ended = false;
        }

        return super.preprocessIndex(index);
    }
}

interface SimpleReaderOptions extends HlsSegmentFetcherOptions, HlsSegmentStreamerOptions, HlsPlaylistFetcherOptions {

    stopDate?: Date | string | number;
}

const createSimpleReader = function (uri: URL | string, options: SimpleReaderOptions = {}): HlsSegmentStreamer {

    options.withData ?? (options.withData = false);

    const playlistFetcherClass = options.stopDate ? UnendingPlaylistFetcher : HlsPlaylistFetcher;

    let readable: ReadableStream<HlsFetcherObject> = new HlsSegmentReadable(new HlsSegmentFetcher(new playlistFetcherClass(uri, options), options));

    if (options.stopDate) {
        const stopDate = new Date(options.stopDate);
        readable = readable.pipeThrough(new TransformStream({
            transform(obj, controller) {

                if ((obj.entry.program_time || 0) > stopDate) {
                    return controller.terminate();
                }

                controller.enqueue(obj);
            }
        }));
    }

    const streamer = new HlsSegmentStreamer(readable, options);

    return streamer;
};

export { createSimpleReader, HlsSegmentReadable, HlsSegmentStreamer };
export type { SimpleReaderOptions };

export default createSimpleReader;