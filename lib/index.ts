import { HlsSegmentReader, HlsSegmentReaderOptions } from './segment-reader';
import { HlsSegmentStreamer, HlsSegmentStreamerOptions } from './segment-streamer';

const exp = function createSimpleReader(uri: string, options: HlsSegmentReaderOptions & HlsSegmentStreamerOptions = {}): HlsSegmentStreamer {

    const reader = new HlsSegmentReader(uri, options);

    options.withData ??= false;

    return new HlsSegmentStreamer(reader, options);
};

exp.HlsSegmentReader = HlsSegmentReader;

exp.HlsSegmentStreamer = HlsSegmentStreamer;

export = exp;
