'use strict';

module.exports = class HlsSegmentObject {

    constructor(fileMeta, stream, seq, details) {

        const isSegment = +seq >= 0;

        this.type = isSegment ? 'segment' : 'init';
        this.file = fileMeta;
        this.stream = stream;

        this.segment = isSegment ? { seq, details } : null;
        this.init = isSegment ? null : details;
    }
};
