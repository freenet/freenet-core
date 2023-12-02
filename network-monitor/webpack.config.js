const path = require("path");

module.exports = {
  mode: "development",
  entry: {
    app: path.resolve(__dirname, "src", "app.ts"),
  },
  devtool: "source-map",
  output: {
    filename: "bundle.js",
    path: path.resolve(__dirname, "dist"),
  },
  resolve: {
    extensions: [".ts", ".tsx", ".js", ".jsx"],
  },
  devServer: {
    static: path.resolve(__dirname, "dist"),
    compress: true,
    port: 9000,
    hot: true,
  },
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: "ts-loader",
        exclude: ["/node_modules/"],
      },
    ],
  },
};
