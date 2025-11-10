const pool = require("../db");

const createTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS daily_differences (
      id SERIAL PRIMARY KEY,
      date VARCHAR(255) UNIQUE NOT NULL,
      added TEXT[] DEFAULT '{}',
      removed TEXT[] DEFAULT '{}',
      total_changes INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_daily_diff_date ON daily_differences(date DESC)`);
};

const find = async (query = {}, options = {}) => {
  let sql = "SELECT * FROM daily_differences";
  const params = [];
  const conditions = [];

  if (query.date) {
    conditions.push(`date = $${params.length + 1}`);
    params.push(query.date);
  }

  if (conditions.length > 0) {
    sql += " WHERE " + conditions.join(" AND ");
  }

  sql += " ORDER BY date DESC";

  if (options.limit) {
    sql += ` LIMIT $${params.length + 1}`;
    params.push(options.limit);
  }

  const result = await pool.query(sql, params);
  return { rows: result.rows };
};

const findOneAndUpdate = async (query, update, options = {}) => {
  const existing = await findOne(query);
  
  if (existing) {
    const updateData = update.$set || update;
    const updateFields = [];
    const params = [];
    let paramIndex = 1;

    if (updateData.added !== undefined) {
      updateFields.push(`added = $${paramIndex}`);
      params.push(Array.isArray(updateData.added) ? updateData.added : []);
      paramIndex++;
    }

    if (updateData.removed !== undefined) {
      updateFields.push(`removed = $${paramIndex}`);
      params.push(Array.isArray(updateData.removed) ? updateData.removed : []);
      paramIndex++;
    }

    if (updateData.totalChanges !== undefined) {
      updateFields.push(`total_changes = $${paramIndex}`);
      params.push(updateData.totalChanges);
      paramIndex++;
    }

    updateFields.push(`updated_at = $${paramIndex}`);
    params.push(new Date());
    paramIndex++;

    params.push(existing.id);

    const sql = `UPDATE daily_differences SET ${updateFields.join(", ")} WHERE id = $${paramIndex} RETURNING *`;
    const result = await pool.query(sql, params);
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
  const sql = `INSERT INTO daily_differences (date, added, removed, total_changes, created_at, updated_at) 
               VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`;
  const params = [
    data.date,
    Array.isArray(data.added) ? data.added : [],
    Array.isArray(data.removed) ? data.removed : [],
    data.totalChanges || data.total_changes || 0,
    data.createdAt || new Date(),
    data.updatedAt || new Date(),
  ];
  const result = await pool.query(sql, params);
  return result.rows[0];
};

const deleteMany = async (query = {}) => {
  let sql = "DELETE FROM daily_differences";
  const params = [];
  const conditions = [];

  if (Object.keys(query).length > 0) {
    Object.keys(query).forEach((key) => {
      conditions.push(`${key} = $${params.length + 1}`);
      params.push(query[key]);
    });
  }

  if (conditions.length > 0) {
    sql += " WHERE " + conditions.join(" AND ");
  }

  const result = await pool.query(sql, params);
  return { deletedCount: result.rowCount };
};

const insertMany = async (dataArray) => {
  const sql = `INSERT INTO daily_differences (date, added, removed, total_changes, created_at, updated_at) 
               VALUES ${dataArray.map((_, i) => {
                 const base = i * 6;
                 return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6})`;
               }).join(", ")} RETURNING *`;
  
  const params = dataArray.flatMap(data => [
    data.date,
    Array.isArray(data.added) ? data.added : [],
    Array.isArray(data.removed) ? data.removed : [],
    data.totalChanges || 0,
    data.createdAt ? new Date(data.createdAt) : new Date(),
    data.updatedAt ? new Date(data.updatedAt) : new Date(),
  ]);

  const result = await pool.query(sql, params);
  return result.rows;
};

module.exports = {
  createTable,
  find,
  findOne,
  findOneAndUpdate,
  create,
  deleteMany,
  insertMany,
};
