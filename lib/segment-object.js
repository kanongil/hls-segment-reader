/* eslint-env node, es6 */

'use strict';

module.exports = function HlsSegmentObject(seq, segment, fileMeta, stream) {

    this.seq = seq;
    this.details = segment;
    this.file = fileMeta;
    this.stream = stream;
};
