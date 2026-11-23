/**
 * rekindle's two tables. They live next to Mastra's own tables (same schema by default), so they
 * isolate per environment along with everything else.
 *
 *  - rekindle_instances: one row per process, refreshed by its heartbeat.
 *  - rekindle_run_owners: one row per opted-in run. The row is the lock.
 */

const IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]*$/

/** The schema is interpolated into SQL, so only a plain identifier is accepted. */
export function assertSchemaName(schema: string | undefined): string {
  if (typeof schema !== 'string' || !IDENTIFIER.test(schema)) {
    throw new Error(`rekindle: invalid schema name "${schema}" (letters, digits and _ only)`)
  }
  return schema
}

/** Quoted, so a mixed-case name means the same schema it does to Mastra. */
export function quoteSchema(schema: string): string {
  return `"${assertSchemaName(schema)}"`
}

/** The SQL `migrate()` runs. Use it to apply the tables through your own migrations instead. */
export function migrationSql(schema: string): string {
  const name = assertSchemaName(schema)
  const s = quoteSchema(name)
  // Sent as one multi-statement query, which Postgres runs as one transaction. The advisory lock
  // makes instances that boot together migrate one after another instead of colliding on
  // CREATE SCHEMA / CREATE TABLE.
  return `
SELECT pg_advisory_xact_lock(hashtext('mastra-rekindle:migrate'));

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = '${name}') THEN
    CREATE SCHEMA ${s};
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS ${s}.rekindle_instances (
  instance_id   text PRIMARY KEY,
  heartbeat_at  timestamptz NOT NULL DEFAULT now(),
  started_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rekindle_instances_heartbeat_idx ON ${s}.rekindle_instances (heartbeat_at);

CREATE TABLE IF NOT EXISTS ${s}.rekindle_run_owners (
  workflow_name     text NOT NULL,
  run_id            text NOT NULL,
  owner_instance_id text REFERENCES ${s}.rekindle_instances(instance_id) ON DELETE SET NULL,
  claimed_at        timestamptz NOT NULL DEFAULT now(),
  restarted_at      timestamptz,
  attempts          integer NOT NULL DEFAULT 0,
  done_at           timestamptz,
  PRIMARY KEY (workflow_name, run_id)
);
CREATE INDEX IF NOT EXISTS rekindle_run_owners_owner_idx ON ${s}.rekindle_run_owners (owner_instance_id);
`
}
