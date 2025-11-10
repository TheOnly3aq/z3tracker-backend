const pool = require("../db");

const createTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS monthly_counts (
      id SERIAL PRIMARY KEY,
      month VARCHAR(255) UNIQUE NOT NULL,
      count INTEGER NOT NULL
    )
  `);
};

const find = async (query = {}) => {
  let sql = "SELECT * FROM monthly_counts";
  const params = [];
  const conditions = [];

  if (query.month) {
    conditions.push(`month = $${params.length + 1}`);
    params.push(query.month);
  }

  if (conditions.length > 0) {
    sql += " WHERE " + conditions.join(" AND ");
  }

  sql += " ORDER BY month ASC";

  const result = await pool.query(sql, params);
  return { rows: result.rows };
};

const findOneAndUpdate = async (query, update, options = {}) => {
  const existing = await findOne(query);
  
  if (existing) {
    const updateData = update.$set || update;
    const sql = `UPDATE monthly_counts SET count = $1 WHERE month = $2 RETURNING *`;
    const result = await pool.query(sql, [updateData.count, query.month]);
    return result.rows[0];
  } else if (options.upsert) {
    const data = { ...query, ...(update.$set || update) };
    return await create(data);
  }
  
  return null;
};

const findOne = async (query = {}) => {
  const result = await find(query);
  return result.rows[0] || null;
};

const create = async (data) => {
  const sql = `INSERT INTO monthly_counts (month, count) VALUES ($1, $2) RETURNING *`;
  const result = await pool.query(sql, [data.month, data.count]);
  return result.rows[0];
};

module.exports = {
  createTable,
  find,
  findOne,
  findOneAndUpdate,
  create,
};
