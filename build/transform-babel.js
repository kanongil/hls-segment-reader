'use strict';

const Babel = require('@babel/core');
const Path = require('path');

const transform = function (content, filename) {

    if (/node_modules/.test(filename)) {
        return content;
    }

    const { code } = Babel.transformSync(content, {
        extends: Path.join(__dirname, '.babelrc'),
        filename,
        sourceFileName: filename,
        sourceMaps: 'inline',
        auxiliaryCommentBefore: '$lab:coverage:off$',
        auxiliaryCommentAfter: '$lab:coverage:on$'
    });

    return code;
};

module.exports = ['js', 'jsx', 'mjs', 'es', 'es6']
    .map((ext) => ({ ext, transform }));
