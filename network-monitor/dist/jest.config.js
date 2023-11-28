"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const config = {
  verbose: true,
  transform: {
    "^.+\\.tsx?$": "ts-jest",
  },
  roots: ["<rootDir>/tests", "<rootDir>/src"],
  moduleNameMapper: {
    "^(.*)\\.js$": "$1",
  },
  testEnvironment: "jsdom",
};
exports.default = config;
//# sourceMappingURL=jest.config.js.map
