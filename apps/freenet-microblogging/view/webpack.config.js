const path = require('path');

module.exports = {
    entry: './web/src/handleContractUpdates.ts',
    devtool: 'inline-source-map',
    module: {
        rules: [{
            test: /\.tsx?$/,
            use: 'ts-loader',
            exclude: /node_modules/,
        }]
    },
    output: {
        filename: 'bundle.js',
        path: path.resolve(__dirname, 'web/dist'),
    },
    resolve: {
        extensions: ['.tsx', '.ts', '.js']
    }
};