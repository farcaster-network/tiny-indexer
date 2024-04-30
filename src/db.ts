import { fileURLToPath } from "node:url";
import path from "path";
import type { Logger } from "./log";
import { promises as fs } from "fs";
import type { HubTables } from "@farcaster/shuttle";
import { FileMigrationProvider, Migrator, Kysely } from "kysely";
import { err, ok, type Result } from "neverthrow";

export async function ensureMigrations(db: Kysely<HubTables>, log: Logger) {
  const result = await migrateToLatest(db, log);
  if (result.isErr()) {
    log.error("Failed to migrate database", result.error);
    throw result.error;
  }
}

const createMigrator = async (db: Kysely<HubTables>, log: Logger) => {
  const currentDir = path.dirname(fileURLToPath(import.meta.url));
  const migrator = new Migrator({
    db,
    provider: new FileMigrationProvider({
      fs,
      path,
      migrationFolder: path.join(currentDir, "migrations"),
    }),
  });

  return migrator;
};

export const migrateToLatest = async (
  db: Kysely<HubTables>,
  log: Logger
): Promise<Result<void, unknown>> => {
  const migrator = await createMigrator(db, log);

  const { error, results } = await migrator.migrateToLatest();

  results?.forEach((it) => {
    if (it.status === "Success") {
      log.info(`Migration "${it.migrationName}" was executed successfully`);
    } else if (it.status === "Error") {
      log.error(`failed to execute migration "${it.migrationName}"`);
    }
  });

  if (error) {
    log.error("Failed to apply all database migrations");
    log.error(error);
    return err(error);
  }

  log.info("Migrations up to date");
  return ok(undefined);
};
