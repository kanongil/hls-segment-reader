import { HlsSegmentReader, HlsReaderObject, HlsSegmentReaderOptions } from './segment-reader';
import { HlsSegmentStreamer, HlsStreamerObject, HlsSegmentStreamerOptions } from './segment-streamer';


const exp = function createSimpleReader(uri: string, options: HlsSegmentReaderOptions & HlsSegmentStreamerOptions = {}): HlsSegmentStreamer {

    const reader = new HlsSegmentReader(uri, options);

    options.withData ??= false;

    const streamer = new HlsSegmentStreamer(reader, options);

    reader.on('problem', (err) => streamer.emit('problem', err));

    return streamer;
};

exp.HlsSegmentReader = HlsSegmentReader;

exp.HlsSegmentStreamer = HlsSegmentStreamer;

exp.HlsReaderObject = HlsReaderObject;

exp.HlsStreamerObject = HlsStreamerObject;

export = exp;
