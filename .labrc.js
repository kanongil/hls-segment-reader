'use strict';

module.exports = {
    transform: require.resolve('./build/transform-typescript'),
    sourcemaps: true,
    flat: true,
    leaks: false
};
