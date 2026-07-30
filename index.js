import 'dotenv/config'
import schema from './database/schema.js';
import initialTableCreation from './database/initial-table-creation.js'
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import { createClient } from '@libsql/client'
import { log } from './logger/index.js'

sqlite3.verbose();

const sourceDbPath = process.env.SOURCE_DATABASE_PATH;
const tursoConnectionString = process.env.TURSO_DATABASE_URL;
const tursoConnectAuthToken = process.env.TURSO_AUTH_TOKEN;

const sourceDb = await open({
  filename: sourceDbPath,
  driver: sqlite3.Database,
});

const tursoDb = createClient({
  url: tursoConnectionString,
  authToken: tursoConnectAuthToken,
});

function quoteIdentifier(identifier) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(identifier)) {
    throw new Error(`Invalid SQL identifier: ${identifier}`);
  }

  return `"${identifier}"`;
}

async function migrateTable(table, primaryColumn, columns = []) {
  log(`\nMigrating table ${table}...`);

  if (!await tableExistsOnTursoDb(table)) {
    log(`Table ${table} does not exist in Turso database. Creating...`);
    await createTableOnTursoDb(table);
  } else {
    log(`Table ${table} already exists in Turso database. Skipping creation...`);
  }

  const sourceQuery = `SELECT * FROM ${quoteIdentifier(table)}`;

  const result = await sourceDb.all(sourceQuery);

  for (const row of result) {
    const commonIdentifier = `${table}[${primaryColumn}] = ${row[primaryColumn]}`;
    const values = columns.map(column => row[column]);
    log(`Migrating row with ${commonIdentifier}`);

    const exists = await keyExistsInTursoDb(table, primaryColumn, row[primaryColumn]);

    if (exists) {
      log(`Row with ${commonIdentifier} already exists in Turso database. Updating...`)
      await updateRowOnTursoDb(table, columns, values);
      continue;
    }

    log(`Row with ${commonIdentifier} does not exist in Turso database. Inserting...`)
    await insertRowOnTursoDb(table, columns, values);
  }
}

async function keyExistsInTursoDb(table, primaryColumn, key) {
  const result = await tursoDb.execute({
    sql: `SELECT 1 FROM ${quoteIdentifier(table)} WHERE ${quoteIdentifier(primaryColumn)} = ? LIMIT 1`,
    args: [key],
  });

  return result.rows.length > 0;
}

async function tableExistsOnTursoDb(tableName) {
  quoteIdentifier(tableName);
  const exists = await tursoDb.execute({
    sql: 'SELECT name FROM sqlite_master WHERE type = ? AND name = ?',
    args: ['table', tableName],
  });

  return exists.rows.length > 0;
}

async function createTableOnTursoDb(tableName) {
  const query = initialTableCreation[tableName];
  await tursoDb.execute(query);
}

async function updateRowOnTursoDb(table, columns = [], values = []) {
  const updateStatements = columns.map(column => `${quoteIdentifier(column)} = ?`);
  const sql = `UPDATE ${quoteIdentifier(table)} SET ${updateStatements.join(', ')} WHERE ${quoteIdentifier(columns[0])} = ?`;

  log(`Updating ${table} row with ${columns.length} bound values`);
  await tursoDb.execute({
    sql,
    args: [...values, values[0]],
  });
}

async function insertRowOnTursoDb(table, columns = [], values = []) {
  const columnsString = columns.map(quoteIdentifier).join(', ');
  const placeholders = columns.map(() => '?').join(', ');
  const sql = `INSERT INTO ${quoteIdentifier(table)} (${columnsString}) VALUES (${placeholders})`;

  log(`Inserting ${table} row with ${columns.length} bound values`);
  await tursoDb.execute({
    sql,
    args: values,
  });
}

(async function main() {
  try {
    console.time('migration-time');

    const migrationPromises = [];

    for (const tableIndex in schema) {
      migrationPromises.push(
        migrateTable(tableIndex, schema[tableIndex][0], schema[tableIndex])
      );
    }

    await Promise.all(migrationPromises);
  } catch (error) {
    console.error('Error during data transfer:', error);
  } finally {
    await sourceDb.close();
    tursoDb.close();

    console.timeEnd('migration-time');
  }
})();
