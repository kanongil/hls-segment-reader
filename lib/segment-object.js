/* eslint-env node, es6 */

'use strict';

module.exports = function HlsSegmentObject(fileMeta, stream, seq, details) {

    const isSegment = +seq >= 0;

    this.type = isSegment ? 'segment' : 'init';
    this.file = fileMeta;
    this.stream = stream;

    this.segment = isSegment ? { seq, details } : null;
    this.init = isSegment ? null : details;
};
