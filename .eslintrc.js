'use strict';

module.exports = {
    root: true,
    extends: [
        '@hapi/hapi',
        'plugin:@typescript-eslint/base',
        'plugin:@typescript-eslint/eslint-recommended',
//        'plugin:@typescript-eslint/recommended'
    ],
    parserOptions: {
        loc: true,
        comment: true,
        range: true,
        ecmaVersion: 2020,
        sourceType: 'script'
    }
};
