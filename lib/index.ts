import { HlsPlaylistReader, HlsPlaylistReaderOptions } from 'hls-playlist-reader';
import { HlsSegmentReadable, HlsSegmentReaderOptions } from './segment-readable';
import { HlsSegmentStreamer, HlsSegmentStreamerOptions } from './segment-streamer';

export { HlsFetcherObject } from './segment-fetcher';
export type { HlsIndexMeta } from 'hls-playlist-reader';
export { HlsStreamerObject } from './segment-streamer';

const createSimpleReader = function (uri: string, options: HlsSegmentReaderOptions & HlsSegmentStreamerOptions = {}): HlsSegmentStreamer {

    const reader = new HlsSegmentReadable(uri, options);

    options.withData ?? (options.withData = false);

    const streamer = new HlsSegmentStreamer(reader, options);

    //reader.on('problem', (err) => streamer.emit('problem', err));

    return streamer;
};

export { createSimpleReader, HlsPlaylistReader, HlsSegmentReadable, HlsSegmentStreamer };
export type { HlsPlaylistReaderOptions, HlsSegmentReaderOptions, HlsSegmentStreamerOptions };

export default createSimpleReader;
