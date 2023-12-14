'use strict';

const EslintPluginHapi = require('@hapi/eslint-plugin');
const TypescriptRules = require('@typescript-eslint/eslint-plugin').rules;


const denylist = new Set([
    'padding-line-between-statements',    // Incompatible syntax
    'key-spacing'                         // Currently broken in node v14: https://github.com/typescript-eslint/typescript-eslint/issues/6396
]);


const tsifyRules = function (from) {

    const rules = {};

    for (const rule in from) {
        if (TypescriptRules[rule] && !denylist.has(rule)) {
            rules[rule] = 'off';
            rules[`@typescript-eslint/${rule}`] = from[rule];
        }
    }

    return rules;
};


const typescript = function (project, files) {

    return {
        files,
        extends: [
            'plugin:@typescript-eslint/recommended'
        ],
        parserOptions: {
            project,
            tsconfigRootDir: __dirname
        },
        rules: {
            ...tsifyRules(EslintPluginHapi.configs.recommended.rules),
            '@typescript-eslint/no-explicit-any': 'off',
            '@typescript-eslint/no-non-null-assertion': 'off',

            '@typescript-eslint/member-delimiter-style': 'warn',
            '@typescript-eslint/no-throw-literal': 'error',
            '@typescript-eslint/prefer-for-of': 'warn',
            '@typescript-eslint/type-annotation-spacing': 'warn',
            '@typescript-eslint/unified-signatures': 'warn',
            '@typescript-eslint/no-floating-promises': ['error', { ignoreVoid: true }],
            '@typescript-eslint/no-misused-promises': 'error'
        }
    };
};

module.exports = {
    root: true,
    extends: [
        'plugin:@hapi/recommended',
        'plugin:@typescript-eslint/eslint-recommended'
    ],
    parserOptions: {
        ecmaVersion: 2022
    },
    ignorePatterns: ['/lib/*.js', '**/*.d.ts', '/test/*.js', '/test/fixtures/**'],
    overrides: [
        typescript('./src/tsconfig.json', ['src/**/*.ts']),
        typescript('./test/tsconfig.json', ['test/*.ts'])
    ]
};
