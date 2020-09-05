'use strict';

/** @typedef { import('./segment-reader').HlsSegmentReaderOptions } HlsSegmentReaderOptions */
/** @typedef { import('./segment-streamer').HlsSegmentStreamerOptions } HlsSegmentStreamerOptions */

const { HlsSegmentReader } = require('./segment-reader');
const { HlsSegmentStreamer } = require('./segment-streamer');

/**
 * @param {string} uri
 * @param {HlsSegmentReaderOptions & HlsSegmentStreamerOptions} [options]
 */
exports = module.exports = function createSimpleReader(uri, options = {}) {

    const reader = new HlsSegmentReader(uri, options);

    if (options.withData === undefined) {
        options.withData = false;
    }

    return new HlsSegmentStreamer(reader, options);
};

exports.HlsSegmentReader = HlsSegmentReader;

exports.HlsSegmentStreamer = HlsSegmentStreamer;
