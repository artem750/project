import globals from 'globals';
import pluginJs from '@eslint/js';
import tseslint from 'typescript-eslint';
//import pluginReact from 'eslint-plugin-react';

export default [
    { files: ['**/*.{js,mjs,cjs,ts,jsx,tsx}'] },
    { languageOptions: { globals: globals.browser } },
    pluginJs.configs.recommended,
    ...tseslint.configs.recommended,
    //pluginReact.configs.flat.recommended, чтобы не проверял везде импорт React from 'react', когда используем jsx
    {
        rules: {
            semi: ['warn', 'always'],
            'object-curly-spacing': ['warn', 'always'],
            'prefer-const': 'warn',
            'no-unused-vars': 'warn',
            'no-var': 'warn',
            indent: ['warn', 4],
        },
    },
];
