const pool = require("../db");

const toSnakeCase = (str) => {
  return str.replace(/([A-Z])/g, "_$1").toLowerCase();
};

const mapFieldsToSnakeCase = (obj) => {
  const mapped = {};
  Object.keys(obj).forEach((key) => {
    if (key === "lastUpdated") {
      mapped.last_updated = obj[key];
    } else {
      mapped[toSnakeCase(key)] = obj[key];
    }
  });
  return mapped;
};

const createTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS rdw_entries (
      id SERIAL PRIMARY KEY,
      kenteken VARCHAR(255) UNIQUE NOT NULL,
      voertuigsoort VARCHAR(255),
      merk VARCHAR(255),
      handelsbenaming VARCHAR(255),
      vervaldatum_apk VARCHAR(255),
      datum_tenaamstelling VARCHAR(255),
      bruto_bpm VARCHAR(255),
      inrichting VARCHAR(255),
      aantal_zitplaatsen VARCHAR(255),
      eerste_kleur VARCHAR(255),
      tweede_kleur VARCHAR(255),
      aantal_cilinders VARCHAR(255),
      cilinderinhoud VARCHAR(255),
      massa_ledig_voertuig VARCHAR(255),
      toegestane_maximum_massa_voertuig VARCHAR(255),
      massa_rijklaar VARCHAR(255),
      datum_eerste_toelating VARCHAR(255),
      datum_eerste_tenaamstelling_in_nederland VARCHAR(255),
      wacht_op_keuren VARCHAR(255),
      catalogusprijs VARCHAR(255),
      wam_verzekerd VARCHAR(255),
      aantal_deuren VARCHAR(255),
      aantal_wielen VARCHAR(255),
      lengte VARCHAR(255),
      maximale_constructiesnelheid VARCHAR(255),
      europese_voertuigcategorie VARCHAR(255),
      plaats_chassisnummer VARCHAR(255),
      technische_max_massa_voertuig VARCHAR(255),
      type VARCHAR(255),
      typegoedkeuringsnummer VARCHAR(255),
      variant VARCHAR(255),
      uitvoering VARCHAR(255),
      volgnummer_wijziging_eu_typegoedkeuring VARCHAR(255),
      vermogen_massarijklaar VARCHAR(255),
      nettomaximumvermogen VARCHAR(255),
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_rdw_kenteken ON rdw_entries(kenteken)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_rdw_last_updated ON rdw_entries(last_updated DESC)`);
};

const find = async (query = {}) => {
  let sql = "SELECT * FROM rdw_entries";
  const params = [];
  const conditions = [];

  if (query.kenteken) {
    conditions.push(`kenteken = $${params.length + 1}`);
    params.push(query.kenteken);
  }

  if (query.$or) {
    const orConditions = [];
    query.$or.forEach((orQuery) => {
      Object.keys(orQuery).forEach((field) => {
        const regex = orQuery[field].$regex;
        const paramIndex = params.length + 1;
        orConditions.push(`${field} ILIKE $${paramIndex}`);
        params.push(`%${regex}%`);
      });
    });
    if (orConditions.length > 0) {
      conditions.push(`(${orConditions.join(" OR ")})`);
    }
  }

  if (conditions.length > 0) {
    sql += " WHERE " + conditions.join(" AND ");
  }

  return pool.query(sql, params);
};

const findOne = async (query = {}) => {
  const result = await find(query);
  return result.rows[0] || null;
};

const findOneAndUpdate = async (query, update, options = {}) => {
  const existing = await findOne(query);
  
  if (existing) {
    const updateFields = [];
    const params = [];
    let paramIndex = 1;

    const updateData = { ...update };
    if (update.$set) {
      Object.assign(updateData, update.$set);
      delete updateData.$set;
    }

    const mappedData = mapFieldsToSnakeCase(updateData);
    Object.keys(mappedData).forEach((key) => {
      updateFields.push(`${key} = $${paramIndex}`);
      params.push(mappedData[key]);
      paramIndex++;
    });

    updateFields.push(`updated_at = $${paramIndex}`);
    params.push(new Date());
    paramIndex++;

    params.push(existing.id);

    const sql = `UPDATE rdw_entries SET ${updateFields.join(", ")} WHERE id = $${paramIndex}`;
    await pool.query(sql, params);
    return await findOne({ id: existing.id });
  } else if (options.upsert) {
    const data = { ...query, ...update };
    if (update.$set) {
      Object.assign(data, update.$set);
    }
    return await create(data);
  }
  
  return null;
};

const create = async (data) => {
  const cleanData = { ...data };
  delete cleanData.$set;
  const mappedData = mapFieldsToSnakeCase(cleanData);
  const fields = Object.keys(mappedData);
  const values = fields.map((_, i) => `$${i + 1}`);
  const params = fields.map(field => mappedData[field]);

  const sql = `INSERT INTO rdw_entries (${fields.join(", ")}) VALUES (${values.join(", ")}) RETURNING *`;
  const result = await pool.query(sql, params);
  return result.rows[0];
};

const deleteMany = async (query) => {
  let sql = "DELETE FROM rdw_entries";
  const params = [];
  const conditions = [];

  if (query.kenteken && query.kenteken.$nin) {
    conditions.push(`kenteken NOT IN (${query.kenteken.$nin.map((_, i) => `$${i + 1}`).join(", ")})`);
    params.push(...query.kenteken.$nin);
  }

  if (conditions.length > 0) {
    sql += " WHERE " + conditions.join(" AND ");
  }

  const result = await pool.query(sql, params);
  return { deletedCount: result.rowCount };
};

const distinct = async (field) => {
  const result = await pool.query(`SELECT DISTINCT ${field} FROM rdw_entries WHERE ${field} IS NOT NULL`);
  return result.rows.map(row => row[field]);
};

const countDocuments = async (query = {}) => {
  let sql = "SELECT COUNT(*) FROM rdw_entries";
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
  return parseInt(result.rows[0].count);
};

const aggregate = async (pipeline) => {
  if (pipeline[0].$group) {
    const groupField = pipeline[0].$group._id.replace("$", "");
    let sql = `SELECT ${groupField} as _id, COUNT(*)::integer as count FROM rdw_entries WHERE ${groupField} IS NOT NULL GROUP BY ${groupField}`;
    
    if (pipeline[1] && pipeline[1].$sort) {
      const sortField = Object.keys(pipeline[1].$sort)[0];
      const sortOrder = pipeline[1].$sort[sortField] === -1 ? "DESC" : "ASC";
      sql += ` ORDER BY ${sortField === "count" ? "count" : groupField} ${sortOrder}`;
    }
    
    if (pipeline[2] && pipeline[2].$limit) {
      sql += ` LIMIT ${pipeline[2].$limit}`;
    }
    
    const result = await pool.query(sql);
    return result.rows.map(row => ({ _id: row._id, count: parseInt(row.count) }));
  }
  
  if (pipeline[0].$addFields) {
    const yearField = pipeline[0].$addFields.year;
    const extractYear = yearField.$substr[0].replace("$", "");
    const sql = `SELECT SUBSTRING(${extractYear}, 1, 4) as _id, COUNT(*)::integer as count FROM rdw_entries WHERE ${extractYear} IS NOT NULL GROUP BY SUBSTRING(${extractYear}, 1, 4) ORDER BY _id`;
    const result = await pool.query(sql);
    return result.rows.map(row => ({ _id: row._id, count: parseInt(row.count) }));
  }
  
  return [];
};

module.exports = {
  createTable,
  find,
  findOne,
  findOneAndUpdate,
  create,
  deleteMany,
  distinct,
  countDocuments,
  aggregate,
};
