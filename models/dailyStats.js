const pool = require("../db");

const createTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS daily_stats (
      id SERIAL PRIMARY KEY,
      date VARCHAR(255) UNIQUE NOT NULL,
      total_vehicles INTEGER NOT NULL,
      insured_count INTEGER NOT NULL,
      imported_count INTEGER NOT NULL,
      color_counts JSONB NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats(date DESC)`
  );

  await pool.query(`
    DO $$ 
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='daily_stats' AND column_name='imported_count') THEN
        ALTER TABLE daily_stats ADD COLUMN imported_count INTEGER DEFAULT 0;
      END IF;
    END $$;
  `);
};

const findOne = async (query = {}) => {
  let sql = "SELECT * FROM daily_stats";
  const params = [];
  const conditions = [];

  if (query.date) {
    conditions.push(`date = $${params.length + 1}`);
    params.push(query.date);
  }

  if (conditions.length > 0) {
    sql += " WHERE " + conditions.join(" AND ");
  }

  sql += " ORDER BY date DESC LIMIT 1";

  const result = await pool.query(sql, params);
  return result.rows[0] || null;
};

const findOneAndUpdate = async (query, update, options = {}) => {
  const existing = await findOne(query);

  if (existing) {
    const updateData = update.$set || update;
    const sql = `UPDATE daily_stats SET total_vehicles = $1, insured_count = $2, imported_count = $3, color_counts = $4, updated_at = $5 WHERE date = $6 RETURNING *`;
    const result = await pool.query(sql, [
      updateData.total_vehicles,
      updateData.insured_count,
      updateData.imported_count,
      JSON.stringify(updateData.color_counts),
      new Date(),
      query.date,
    ]);
    return result.rows[0];
  } else if (options.upsert) {
    const data = { ...query, ...(update.$set || update) };
    return await create(data);
  }

  return null;
};

const create = async (data) => {
  const sql = `INSERT INTO daily_stats (date, total_vehicles, insured_count, imported_count, color_counts, created_at, updated_at) 
               VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`;
  const params = [
    data.date,
    data.total_vehicles,
    data.insured_count,
    data.imported_count,
    JSON.stringify(data.color_counts),
    data.createdAt || new Date(),
    data.updatedAt || new Date(),
  ];
  const result = await pool.query(sql, params);
  return result.rows[0];
};

module.exports = {
  createTable,
  findOne,
  findOneAndUpdate,
  create,
};
