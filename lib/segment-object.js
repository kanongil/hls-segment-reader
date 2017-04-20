/* eslint-env node, es6 */

'use strict';

module.exports = function HlsSegmentObject(fileMeta, stream, seq, segment) {

    this.type = segment ? 'segment' : 'init';
    this.file = fileMeta;
    this.stream = stream;

    this.segment = segment ? { seq, details: segment } : null;
};
