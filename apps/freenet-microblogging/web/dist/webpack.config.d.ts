export const mode: string;
export const entry: string;
export const devtool: string;
export namespace output {
    const filename: string;
    const path: string;
}
export namespace resolve {
    const extensions: string[];
}
export namespace devServer {
    const _static: string;
    export { _static as static };
    export const port: number;
    export const hot: boolean;
}
export namespace module {
    const rules: ({
        test: RegExp;
        use: string;
        exclude: RegExp;
    } | {
        test: RegExp;
        use: ({
            loader: string;
            options?: undefined;
        } | {
            loader: string;
            options: {
                postcssOptions: {
                    plugins: () => typeof import("autoprefixer")[];
                };
            };
        })[];
        exclude?: undefined;
    })[];
}
