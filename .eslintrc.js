'use strict';

module.exports = {
    extends: '@hapi/hapi',
    parser: 'babel-eslint',
    parserOptions: {
        loc: true,
        comment: true,
        range: true,
        ecmaVersion: 2019,
        sourceType: 'script'
    }
};
