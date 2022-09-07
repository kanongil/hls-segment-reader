import { HlsSegmentReadable, HlsSegmentReaderOptions } from './segment-readable.js';
import { HlsSegmentStreamer, HlsSegmentStreamerOptions } from './segment-streamer.js';

export { HlsFetcherObject } from './segment-fetcher.js';
export type { HlsIndexMeta } from 'hls-playlist-reader';
export { HlsStreamerObject } from './segment-streamer.js';

const createSimpleReader = function (uri: string, options: HlsSegmentReaderOptions & HlsSegmentStreamerOptions = {}): HlsSegmentStreamer {

    const reader = new HlsSegmentReadable(uri, options);

    options.withData ?? (options.withData = false);

    const streamer = new HlsSegmentStreamer(uri, options);

    //reader.on('problem', (err) => streamer.emit('problem', err));

    return streamer;
};

export { createSimpleReader, HlsSegmentReadable, HlsSegmentStreamer };
export type { HlsSegmentReaderOptions, HlsSegmentStreamerOptions };

export default createSimpleReader;
