/**
 * The slice of a logger rekindle uses. Mastra's logger (`mastra.getLogger()`) fits, as do
 * pino and console-shaped loggers.
 */
export interface RekindleLogger {
  info(message: string, meta?: Record<string, unknown>): void
  warn(message: string, meta?: Record<string, unknown>): void
  error(message: string, meta?: Record<string, unknown>): void
}

export const consoleLogger: RekindleLogger = {
  info: (message, meta) => console.info(`[rekindle] ${message}`, meta ?? ''),
  warn: (message, meta) => console.warn(`[rekindle] ${message}`, meta ?? ''),
  error: (message, meta) => console.error(`[rekindle] ${message}`, meta ?? ''),
}

export function errorFields(err: unknown): Record<string, unknown> {
  return err instanceof Error
    ? { error: err.message, errorName: err.name, stack: err.stack }
    : { error: String(err) }
}
