import pg from 'pg';

const { Pool } = pg;

export function createDbPool(): pg.Pool {
  const connectionString = process.env.SUPABASE_DB_URL;
  if (!connectionString) {
    throw new Error('SUPABASE_DB_URL env variable is required');
  }

  return new Pool({
    connectionString,
    ssl: { rejectUnauthorized: false },
    max: 5,
  });
}

export const IS_SHADOW_MODE = process.env.SHADOW_MODE === '1';

export function tableName(baseName: string): string {
  return IS_SHADOW_MODE ? `${baseName}_staging` : baseName;
}
