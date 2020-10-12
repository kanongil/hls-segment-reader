'use strict';

const { EventEmitter } = require('events');
const { Duplex, Readable, Transform/*, Writable*/ } = require('readable-stream');


const internals = {
    destroyOnce(Base) {

        return class OnceDestroy extends Base {

            destroy(...args) {

                if (!this.destroyed) {
                    return super.destroy(...args);
                }

                return this;
            }
        };
    }
};


exports.TypedEmitter = function TypedEmitter(_, Base = EventEmitter) {

    return internals.destroyOnce(Base);
};

exports.TypedReadable = function TypedReadable(_, Base = Readable) {

    return internals.destroyOnce(Base);
};

/*exports.TypedWritable = function TypedWritable(_, Base = Writable) {

    return internals.destroyOnce(Base);
};*/

exports.TypedDuplex = function TypedDuplex(W, R, Base = Duplex) {

    return internals.destroyOnce(Base);
};

exports.TypedTransform = function TypedTransform(W, R, Base = Transform) {

    return internals.destroyOnce(Base);
};
