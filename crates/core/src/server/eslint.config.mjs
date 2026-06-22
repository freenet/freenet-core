import js from '@eslint/js';
import globals from 'globals';

// Lints the browser-side JS that the gateway server serves to clients
// (extracted from Rust raw-string consts in issue #4017). The scripts are
// classic, non-module browser scripts; entry-point functions are invoked from
// inline HTML handlers that ESLint cannot see.
export default [
  { ignores: ['node_modules/**'] },
  js.configs.recommended,
  {
    files: ['**/assets/**/*.js'],
    languageOptions: {
      ecmaVersion: 2021,
      sourceType: 'script',
      globals: {
        ...globals.browser,
        // Minted by shell_user_token.js and read by the shell bridge / hosted
        // bar (separately injected <script> tags share the global scope).
        __freenet_user_token: 'readonly',
      },
    },
    rules: {
      // catch(e) {} with an unused binding is a deliberate "ignore" idiom here.
      'no-unused-vars': ['error', { vars: 'local', args: 'none', caughtErrors: 'none' }],
      'no-empty': ['error', { allowEmptyCatch: true }],
      // These scripts predate `let`/`const` and rely on function-scoped `var`,
      // including intentional re-`var` of the same name across switch cases.
      'no-redeclare': 'off',
    },
  },
];
