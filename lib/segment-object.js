'use strict';

/** @typedef { import('m3u8parse/lib/m3u8playlist').M3U8IndependentSegment } M3U8IndependentSegment */
/** @typedef { import('./helpers').FetchResult['meta'] } Meta */
/** @typedef { import('./helpers').ReadableStream } ReadableStream */

const { AttrList, M3U8Segment } = require('m3u8parse');


module.exports = class HlsSegmentObject {

    /**
     * @param {Meta} fileMeta
     * @param {ReadableStream | undefined} [stream]
     * @param {{ msn: number, isMap?: boolean }} ptr
     * @param {M3U8IndependentSegment | AttrList} details
     */
    constructor(fileMeta, stream, ptr, details) {

        const isSegment = !ptr.isMap;

        this.type = isSegment ? 'segment' : 'init';
        this.file = fileMeta;
        this.stream = stream;

        this.segment = isSegment && details instanceof M3U8Segment ? { msn: ptr.msn, details } : null;
        this.init = isSegment || !(details instanceof AttrList) ? null : details;
    }
};
