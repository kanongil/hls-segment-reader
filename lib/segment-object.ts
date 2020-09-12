import type { HlsReaderObject } from './segment-reader';
import type { FetchResult, ReadableStream } from './helpers';

import { AttrList } from 'm3u8parse/lib/attrlist';


export class HlsSegmentObject {

    type: 'segment' | 'map';
    file: FetchResult['meta'];
    stream?: ReadableStream;
    segment?: HlsReaderObject;
    attrs?: AttrList;

    constructor(fileMeta: FetchResult['meta'], stream: ReadableStream | undefined, type: 'map', details: AttrList);
    constructor(fileMeta: FetchResult['meta'], stream: ReadableStream | undefined, type: 'segment', details: HlsReaderObject);

    constructor(fileMeta: FetchResult['meta'], stream: ReadableStream | undefined, type: 'segment' | 'map', details: HlsReaderObject | AttrList) {

        const isSegment = type === 'segment';

        this.type = type;
        this.file = fileMeta;
        this.stream = stream;

        if (isSegment) {
            this.segment = details as HlsReaderObject;
        }
        else {
            this.attrs = details as AttrList;
        }
    }
}
