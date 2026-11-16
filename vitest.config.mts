import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'node:url';

// Тесты гоняем по исходникам: alias на src, чтобы не требовать предварительный build контрактов.
export default defineConfig({
  resolve: {
    alias: {
      '@wfm/contracts': fileURLToPath(
        new URL('./packages/contracts/src/index.ts', import.meta.url),
      ),
      '@wfm/service-kit': fileURLToPath(
        new URL('./packages/service-kit/src/index.ts', import.meta.url),
      ),
    },
  },
  test: {
    include: ['{apps,packages}/*/src/**/*.spec.ts'],
    environment: 'node',
  },
});
