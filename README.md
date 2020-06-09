# HlsSegmentReader for node.js

Read segments from any [Apple HLS](http://tools.ietf.org/html/draft-pantos-http-live-streaming) source in an object-mode `Readable`.

[![Build Status](https://travis-ci.org/kanongil/node-hls-segment-reader.svg?branch=master)](https://travis-ci.org/kanongil/node-hls-segment-reader)

## API

### new HlsSegmentReader(uri, [options])

Creates an `objectMode` `Readable`, which returns segments from the `uri`, as specified in `options`.

#### Options

 * `fullStream` - Always start from first segment. Otherwise, follow standard client behavior.
 * `withData` - Set to open & return data streams for each segment.
 * `startDate` - Select initial segment based on datetime information in the index.
 * `stopDate` - Stop stream after this date based on datetime information in the index.
 * `maxStallTime` - Stop live/event stream if index has not been updated in `maxStallTime` ms.
 * `extensions` - Allow specified index extensions, as specified in `m3u8parse`.

### Event: `index`

 * `index` - `M3U8Playlist` with parsed index.

Emitted whenever a new remote index has been parsed.

### Event: `data`

 * `obj` - `HlsSegmentObject` containing segment data.

### HlsSegmentReader#abort([graceful])

Stop the reader.

### HlsSegmentObject

 * `type` - `'segment'` or `'init'`.
 * `file` - File metadata from remote server.
 * `stream` - `uristream` `Readable` with segment data when `withData` is set.
 * `segment` - Object with segment data, when type is `'segment'`:
   * `seq` - Sequence number.
   * `details` - `M3U8Segment` info.
 * `init` - m3u8 `AttrList` with map segment data when type is `'init'`.

## Installation

```sh
$ npm install hls-segment-reader
```

# License

(BSD 2-Clause License)

Copyright (c) 2014-2020, Gil Pedersen &lt;gpdev@gpost.dk&gt;
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
