import { HlsPlaylistReader, HlsPlaylistReaderOptions } from 'hls-playlist-reader';
import { HlsSegmentReader, HlsSegmentReaderOptions } from './segment-reader';
import { HlsSegmentStreamer, HlsSegmentStreamerOptions } from './segment-streamer';

export { HlsReaderObject } from './segment-reader';
export type { HlsIndexMeta } from 'hls-playlist-reader';
export { HlsStreamerObject } from './segment-streamer';

const createSimpleReader = function (uri: string, options: HlsSegmentReaderOptions & HlsSegmentStreamerOptions = {}): HlsSegmentStreamer {

    const reader = new HlsSegmentReader(uri, options);

    options.withData ??= false;

    const streamer = new HlsSegmentStreamer(reader, options);

    reader.on('problem', (err) => streamer.emit('problem', err));

    return streamer;
};

export { createSimpleReader, HlsPlaylistReader, HlsSegmentReader, HlsSegmentStreamer };
export type { HlsPlaylistReaderOptions, HlsSegmentReaderOptions, HlsSegmentStreamerOptions };

export default createSimpleReader;
