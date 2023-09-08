import type {Config} from '@jest/types';

const config: Config.InitialOptions = {
    verbose: true,
    transform: {
        "^.+\\.tsx?$": "ts-jest",
    },
    roots: ["<rootDir>/tests", "<rootDir>/src"],
    moduleNameMapper: {
        "host-response": "<rootDir>/src/host-response/host-response.ts",
    }
};
export default config;
