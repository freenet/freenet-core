import type { Config } from '@jest/types';
const config: Config.InitialOptions = {
	verbose: true,
	transform: {
		"^.+\\.tsx?$": "ts-jest",
	},
	roots: ["<rootDir>/tests", "<rootDir>/web/src"]
};
export default config;
