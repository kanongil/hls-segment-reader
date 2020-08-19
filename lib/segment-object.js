'use strict';

const { AttrList, M3U8Segment, M3U8IndependentSegment } = require('m3u8parse');

const Helpers = require('./helpers');


module.exports = class HlsSegmentObject {

    /**
     * @param {Helpers.FetchResult['meta']} fileMeta
     * @param {NodeJS.ReadStream} stream
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
