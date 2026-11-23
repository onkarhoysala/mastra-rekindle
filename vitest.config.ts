import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    // Tests share one Postgres; each file uses its own schema, but files run one at a time
    // so timing-sensitive staleness checks are not starved.
    fileParallelism: false,
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
})
