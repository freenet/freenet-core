import type {Config} from '@jest/types';

const config: Config.InitialOptions = {
    verbose: true,
    transform: {
        "^.+\\.tsx?$": "ts-jest",
    },
    roots: ["<rootDir>/tests", "<rootDir>/src"],
    moduleNameMapper: {
        "^(.*)\\.js$": "$1",
    }
};
export default config;
