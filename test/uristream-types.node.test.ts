import type { Meta } from 'uristream/lib/uri-reader.js';

import { expect } from '@hapi/code';
import Uristream from 'uristream';

describe('Uristream', () => {

    it('maps file extensions to suitable mime types', async () => {

        const map = new Map([
            ['audio.aac', 'audio/x-aac'],
            ['audio.ac3', 'audio/ac3'],
            ['audio.dts', 'audio/vnd.dts'],
            ['audio.eac3', 'audio/eac3'],
            ['audio.m4a', 'audio/mp4'],
            ['file.m4s', 'video/iso.segment'],
            ['file.mp4', 'video/mp4'],
            ['text.vtt', 'text/vtt'],
            ['video.m4v', 'video/x-m4v']
        ]);

        for (const [file, mime] of map) {
            // eslint-disable-next-line @typescript-eslint/no-loop-func
            const meta: Meta = await new Promise((resolve, reject) => {

                const uristream = Uristream(new URL(`fixtures/files/${file}`, import.meta.url).href);
                uristream.on('error', reject);
                uristream.on('meta', resolve);
            });

            expect(meta.mime).to.equal(mime);
        }
    });
});
