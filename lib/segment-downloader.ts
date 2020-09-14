import type { URL } from 'url';
import type { Byterange } from './helpers';

import { finished } from 'stream';
import { promisify } from 'util';

import { applyToDefaults, assert } from '@hapi/hoek';

import {  performFetch } from './helpers';


const internals = {
    defaults: {
        probe: false
    },
    streamFinished: promisify(finished)
};


// eslint-disable-next-line @typescript-eslint/ban-types
type FetchToken = object | string | number;

export class SegmentDownloader {

    probe: boolean;

    #fetches = new Map<FetchToken, ReturnType<typeof performFetch>>();

    constructor(options: { probe?: boolean }) {

        options = applyToDefaults(internals.defaults, options);

        this.probe = !!options.probe;
    }

    fetchSegment(token: FetchToken, uri: URL, byterange?: Required<Byterange>, { tries = 3 } = {}): ReturnType<typeof performFetch> {

        const promise = performFetch(uri, { byterange, probe: this.probe, retries: tries - 1 });
        this._startTracking(token, promise);
        return promise;
    }

    /**
     * Stops any fetch not in token list
     *
     * @param {Set<FetchToken>} tokens
     */
    setValid(tokens = new Set()): void {

        for (const [token, fetch] of this.#fetches) {

            if (!tokens.has(token)) {
                this._stopTracking(token);
                fetch.abort();
            }
        }
    }

    private _startTracking(token: FetchToken, promise: ReturnType<typeof performFetch>) {

        assert(!this.#fetches.has(token), 'A token can only be tracked once');

        // Setup auto-untracking

        promise.then(({ stream }) => {

            if (!stream) {
                return this._stopTracking(token);
            }

            if (!this.#fetches.has(token)) {
                return;         // It has already been aborted
            }

            finished(stream, () => this._stopTracking(token));
        }).catch((/*err*/) => {

            this._stopTracking(token);
        });

        this.#fetches.set(token, promise);
    }

    private _stopTracking(token: FetchToken) {

        this.#fetches.delete(token);
    }
}
