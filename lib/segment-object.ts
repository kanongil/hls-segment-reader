import type { M3U8IndependentSegment } from 'm3u8parse/lib/m3u8playlist';
import type { FetchResult, ReadableStream } from './helpers';

import { AttrList } from 'm3u8parse/lib/attrlist';
import { M3U8Segment } from 'm3u8parse/lib/m3u8playlist';


export class HlsSegmentObject {

    type: 'segment' | 'init';
    file: FetchResult['meta'];
    stream?: ReadableStream;
    segment?: { msn: number; details: M3U8IndependentSegment };
    init?: AttrList;

    constructor(fileMeta: FetchResult['meta'], stream: ReadableStream | undefined, ptr: { msn: number, isMap?: boolean }, details: M3U8IndependentSegment | AttrList) {

        const isSegment = !ptr.isMap;

        this.type = isSegment ? 'segment' : 'init';
        this.file = fileMeta;
        this.stream = stream;

        this.segment = isSegment && details instanceof M3U8Segment ? { msn: ptr.msn, details } : undefined;
        this.init = isSegment || !(details instanceof AttrList) ? undefined : details;
    }
};
