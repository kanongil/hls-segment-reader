'use strict';

module.exports = {
    transform: require.resolve('./build/transform-typescript'),
    sourcemaps: true,
    threshold: 90,
    flat: true
};
