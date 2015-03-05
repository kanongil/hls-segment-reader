/* jshint node:true */

"use strict";

var Util = require('util'),
    Url = require('url');

var Oncemore = require('oncemore'),
    M3U8Parse = require('m3u8parse'),
    UriStream = require('uristream');

var Readable = require('readable-stream');

var exports = module.exports = HlsSegmentReader;
exports.HlsSegmentObject = HlsSegmentObject;

function HlsSegmentObject(seq, segment, meta, stream) {
  this.seq = seq;
  this.segment = segment;
  this.meta = meta;
  this.stream = stream;
}

function fetchfrom(reader, seqNo, segment, cb) {
  var segmentUrl = Url.resolve(reader.baseUrl, segment.uri);
  var probe = !!reader.noData;

  var stream = UriStream(segmentUrl, { probe:probe, highWaterMark:100 * 1000 * 1000 });

  function finish(err, res) {
    stream.removeListener('meta', onmeta);
    stream.removeListener('end', onfail);
    stream.removeListener('error', onfail);
    cb(err, res);
  }

  function onmeta(meta) {
    if (reader.segmentMimeTypes.indexOf(meta.mime.toLowerCase()) === -1) {
      if (stream.abort) stream.abort();
      return stream.emit(new Error('Unsupported segment MIME type: ' + meta.mime));
    }

    finish(null, new HlsSegmentObject(seqNo, segment, meta, stream));
  }

  function onfail(err) {
    if (!err) err = new Error('No metadata');
    finish(err);
  }

  stream.on('meta', onmeta);
  stream.on('end', onfail);
  stream.on('error', onfail);

  return stream;
}

function checknext(reader) {
  var state = reader.readState;
  var index = reader.index;
  if (!state.active || state.fetching || state.nextSeq === -1 || !index)
    return null;

  var seq = state.nextSeq;
  var segment = index.getSegment(seq, true);

  if (segment) {
    // mark manual discontinuities
    if (state.discont) {
      segment.discontinuity = true;
      state.discont = false;
    }

    // check if we need to stop
    if (reader.stopDate && segment.program_time > reader.stopDate)
      return reader.push(null);

    state.fetching = fetchfrom(reader, seq, segment, function(err, object) {
      state.fetching = null;
      if (err) reader.emit('error', err);

      if (seq === state.nextSeq)
        state.nextSeq++;

      if (object) {
        reader.watch[object.seq] = object.stream;
        Oncemore(object.stream).once('end', 'error', function() {
          delete reader.watch[object.seq];
        });

        state.active = reader.push(object);
      }

      checknext(reader);
    });
  } else if (index.ended) {
    reader.push(null);
  } else if (!index.type && (index.lastSeqNo() < state.nextSeq - 1)) {
    // handle live stream restart
    state.discont = true;
    state.nextSeq = index.startSeqNo(true);
    checknext(reader);
  }
}

function HlsSegmentReader(src, options) {
  if (!(this instanceof HlsSegmentReader))
    return new HlsSegmentReader(src, options);

  var self = this;

  options = options || {};
  if (typeof src === 'string')
    src = Url.parse(src);

  this.url = src;
  this.baseUrl = src;

  this.fullStream = !!options.fullStream;
  this.noData = !!options.noData;

  // dates are inclusive
  this.startDate = options.startDate ? new Date(options.startDate) : null;
  this.stopDate = options.stopDate ? new Date(options.stopDate) : null;

  this.maxStallTime = options.maxStallTime || Infinity;

  this.extensions = options.extensions || {};

  this.index = null;
  this.readState = {
    nextSeq: -1,
    active: false,
    fetching: null,
    discont: false,
  };
  this.watch = {}; // used to stop buffering on expired segments

  this.indexStallSince = null;

  function getUpdateInterval(updated) {
    if (updated && self.index.segments.length) {
      self.indexStallSince = null;
      return Math.min(self.index.target_duration, self.index.segments[self.index.segments.length - 1].duration);
    } else {
      if (self.indexStallSince !== null) {
        if ((Date.now() - self.indexStallSince) > self.maxStallTime)
          return -1;
      } else {
        self.indexStallSince = Date.now();
      }
      return self.index.target_duration / 2;
    }
  }

  function initialSeqNo() {
    var index = self.index;

    if (self.startDate)
      return index.seqNoForDate(self.startDate, true);
    else
      return index.startSeqNo(self.fullStream);
  }

  function updatecheck(updated) {
    if (updated) {
      if (self.readState.nextSeq === -1)
        self.readState.nextSeq = initialSeqNo();
      else if (self.readState.nextSeq < self.index.startSeqNo(true)) {
        // playlist skipped ahead for whatever reason
        self.readState.discont = true;
        self.readState.nextSeq = self.index.startSeqNo(true);
      }

      // check watched segments
      for (var seq in self.watch) {
        if (!self.index.isValidSeqNo(seq)) {
          var stream = self.watch[seq];
          delete self.watch[seq];

          setTimeout(function () {
            if (!stream.ended && stream.abort) stream.abort();
          }, self.index.target_duration * 1000);
        }
      }

      self.emit('index', self.index);

      if (self.index.variant)
        return self.push(null);
    }
    checknext(self);

    if (self.index && !self.index.ended && self.readable) {
      var updateInterval = getUpdateInterval(updated);
      if (updateInterval <= 0)
        return self.emit('error', new Error('index stall'));

      setTimeout(updateindex, Math.max(1, updateInterval) * 1000);
    }
  }

  function updateindex() {
    if (!self.readable) return;

    var stream = UriStream(Url.format(self.url), { timeout:30 * 1000 });
    stream.on('meta', function(meta) {
      // check for valid mime type
      if (self.indexMimeTypes.indexOf(meta.mime.toLowerCase()) === -1 &&
          meta.url.indexOf('.m3u8', meta.url.length - 5) === -1 &&
          meta.url.indexOf('.m3u', meta.url.length - 4) === -1) {
        // FIXME: correctly handle .m3u us-ascii encoding
        if (stream.abort) stream.abort();
        return stream.emit('error', new Error('Invalid MIME type: ' + meta.mime));
      }

      self.baseUrl = meta.url;
    });

    M3U8Parse(stream, { extensions:self.extensions }, function(err, index) {
      if (err) {
        self.emit('error', err);
        updatecheck(false);
      } else {
        var updated = true;
        if (self.index && self.index.lastSeqNo() === index.lastSeqNo()) {
          updated = false;
        }

        self.index = index;
        updatecheck(updated);
      }
    });
  }

  Readable.call(this, { objectMode:true, highWaterMark:options.highWaterMark || 0 });

  updateindex();
}
Util.inherits(HlsSegmentReader, Readable);

HlsSegmentReader.prototype.destroy = function() {
  if (!this.readable) return;

  this.readable = false;

  if (this.fetching && !this.fetching.ended && this.fetching.abort)
    this.fetching.abort();

  for (var seq in this.watch) {
    var stream = this.watch[seq];
    delete this.watch[seq];
    if (!stream.ended && stream.abort)
      stream.abort();
  }
};

HlsSegmentReader.prototype.indexMimeTypes = [
  'application/vnd.apple.mpegurl',
  'application/x-mpegurl',
  'audio/mpegurl',
];

HlsSegmentReader.prototype.segmentMimeTypes = [
  'video/mp2t',
  'audio/aac',
  'audio/x-aac',
  'audio/ac3',
  'text/vtt',
];

HlsSegmentReader.prototype._read = function(/*n*/) {
  this.readState.active = true;
  checknext(this);
};
