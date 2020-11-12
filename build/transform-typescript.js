'use strict';

const Ts = require('typescript');

const cache = new Map();

const tsify = function (content, fileName) {

    const searchPath = Ts.normalizePath(Ts.sys.getCurrentDirectory());
    const configFileName = process.env.TSCONFIG || Ts.findConfigFile(searchPath, Ts.sys.fileExists);

    const compilerOptions = getCompilerOptionsViaCache(configFileName);
    compilerOptions.sourceMap = false;
    compilerOptions.inlineSourceMap = true;

    const { outputText/*, diagnostics*/ } = Ts.transpileModule(content, {
        fileName,
        compilerOptions,
        reportDiagnostics: false
    });

    const splicePoint = outputText.indexOf('Object.defineProperty(exports, "__esModule", { value: true })');
    if (splicePoint !== -1) {
        return '/* $lab:coverage:off$ */' + outputText.slice(0, splicePoint) + '/* $lab:coverage:on$ */' + outputText.slice(splicePoint);
    }

    return outputText;
};

const getCompilerOptionsViaCache = function (configFileName) {

    let options;

    if (!(options = cache[configFileName])) {
        options = cache[configFileName] = getCompilerOptions(configFileName);
    }

    return options;
};

const getCompilerOptions = function (configFileName) {

    const { config, error } = Ts.readConfigFile(configFileName, Ts.sys.readFile);
    if (error) {
        throw new Error(`TS config error in ${configFileName}: ${error.messageText}`);
    }

    const { options } = Ts.parseJsonConfigFileContent(
        config,
        Ts.sys,
        Ts.getDirectoryPath(configFileName),
        {},
        configFileName);

    return options;
};

module.exports = [{
    ext: '.ts',
    transform: tsify
}];
