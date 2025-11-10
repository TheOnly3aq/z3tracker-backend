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

const ensureColumnExists = async (columnName) => {
  if (!/^[a-z0-9_]+$/.test(columnName)) {
    return;
  }

  const result = await pool.query(
    `
    SELECT EXISTS (
      SELECT 1 FROM information_schema.columns 
      WHERE table_name = 'rdw_entries' AND column_name = $1
    )
  `,
    [columnName]
  );

  if (!result.rows[0].exists) {
    await pool.query(
      `ALTER TABLE rdw_entries ADD COLUMN "${columnName}" VARCHAR(255)`
    );
  }
};

const ensureColumnsExist = async (fields) => {
  for (const field of fields) {
    if (field !== "id" && field !== "created_at" && field !== "updated_at") {
      await ensureColumnExists(field);
    }
  }
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
      maximum_massa_trekken_ongeremd VARCHAR(255),
      wielbasis VARCHAR(255),
      afstand_hart_koppeling_tot_achterzijde_voertuig VARCHAR(255),
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_rdw_kenteken ON rdw_entries(kenteken)`
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_rdw_last_updated ON rdw_entries(last_updated DESC)`
  );

  await pool.query(`
    DO $$ 
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='rdw_entries' AND column_name='maximum_massa_trekken_ongeremd') THEN
        ALTER TABLE rdw_entries ADD COLUMN maximum_massa_trekken_ongeremd VARCHAR(255);
      END IF;
      IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='rdw_entries' AND column_name='wielbasis') THEN
        ALTER TABLE rdw_entries ADD COLUMN wielbasis VARCHAR(255);
      END IF;
      IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='rdw_entries' AND column_name='afstand_hart_koppeling_tot_achterzijde_voertuig') THEN
        ALTER TABLE rdw_entries ADD COLUMN afstand_hart_koppeling_tot_achterzijde_voertuig VARCHAR(255);
      END IF;
    END $$;
  `);
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
    const fields = Object.keys(mappedData);

    await ensureColumnsExist(fields);
    
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

  await ensureColumnsExist(fields);

  const values = fields.map((_, i) => `$${i + 1}`);
  const params = fields.map((field) => mappedData[field]);

  const sql = `INSERT INTO rdw_entries (${fields.join(
    ", "
  )}) VALUES (${values.join(", ")}) RETURNING *`;
  const result = await pool.query(sql, params);
  return result.rows[0];
};

const deleteMany = async (query) => {
  let sql = "DELETE FROM rdw_entries";
  const params = [];
  const conditions = [];

  if (query.kenteken && query.kenteken.$nin) {
    conditions.push(
      `kenteken NOT IN (${query.kenteken.$nin
        .map((_, i) => `$${i + 1}`)
        .join(", ")})`
    );
    params.push(...query.kenteken.$nin);
  }

  if (conditions.length > 0) {
    sql += " WHERE " + conditions.join(" AND ");
  }

  const result = await pool.query(sql, params);
  return { deletedCount: result.rowCount };
};

const distinct = async (field) => {
  const result = await pool.query(
    `SELECT DISTINCT ${field} FROM rdw_entries WHERE ${field} IS NOT NULL`
  );
  return result.rows.map((row) => row[field]);
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
      sql += ` ORDER BY ${
        sortField === "count" ? "count" : groupField
      } ${sortOrder}`;
    }

    if (pipeline[2] && pipeline[2].$limit) {
      sql += ` LIMIT ${pipeline[2].$limit}`;
    }

    const result = await pool.query(sql);
    return result.rows.map((row) => ({
      _id: row._id,
      count: parseInt(row.count),
    }));
  }

  if (pipeline[0].$addFields) {
    const yearField = pipeline[0].$addFields.year;
    const extractYear = yearField.$substr[0].replace("$", "");
    const sql = `SELECT SUBSTRING(${extractYear}, 1, 4) as _id, COUNT(*)::integer as count FROM rdw_entries WHERE ${extractYear} IS NOT NULL GROUP BY SUBSTRING(${extractYear}, 1, 4) ORDER BY _id`;
    const result = await pool.query(sql);
    return result.rows.map((row) => ({
      _id: row._id,
      count: parseInt(row.count),
    }));
  }

  return [];
};

const bulkUpsert = async (entries, onProgress) => {
  if (entries.length === 0)
    return { newCount: 0, updatedCount: 0, addedKentekens: [] };

  const existingKentekens = await distinct("kenteken");
  const existingSet = new Set(existingKentekens);

  const toInsert = [];
  const toUpdate = [];
  const addedKentekens = [];

  for (const entry of entries) {
    const mapped = mapFieldsToSnakeCase({ ...entry, lastUpdated: new Date() });
    if (existingSet.has(entry.kenteken)) {
      toUpdate.push(mapped);
    } else {
      toInsert.push(mapped);
      addedKentekens.push(entry.kenteken);
    }
  }

  if (toInsert.length > 0) {
    const firstEntry = toInsert[0];
    const fields = Object.keys(firstEntry);
    await ensureColumnsExist(fields);

    const batchSize = 100;
    for (let i = 0; i < toInsert.length; i += batchSize) {
      const batch = toInsert.slice(i, i + batchSize);
      const values = batch
        .map((entry, idx) => {
          const base = idx * fields.length;
          return `(${fields
            .map((_, fIdx) => `$${base + fIdx + 1}`)
            .join(", ")})`;
        })
        .join(", ");

      const params = batch.flatMap((entry) =>
        fields.map((field) => entry[field] || null)
      );
      const sql = `INSERT INTO rdw_entries (${fields
        .map((f) => `"${f}"`)
        .join(", ")}) VALUES ${values} ON CONFLICT (kenteken) DO NOTHING`;
      await pool.query(sql, params);

      if (onProgress) {
        onProgress(i + batch.length, toInsert.length, "inserting");
      }
    }
  }

  if (toUpdate.length > 0) {
    const firstEntry = toUpdate[0];
    const fields = Object.keys(firstEntry).filter(
      (f) => f !== "kenteken" && f !== "id" && f !== "created_at"
    );
    await ensureColumnsExist([...fields, "kenteken"]);

    const batchSize = 100;
    for (let i = 0; i < toUpdate.length; i += batchSize) {
      const batch = toUpdate.slice(i, i + batchSize);

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        for (const entry of batch) {
          const updateFields = fields
            .map((f, idx) => `"${f}" = $${idx + 1}`)
            .join(", ");
          const params = [
            ...fields.map((f) => entry[f] || null),
            new Date(),
            entry.kenteken,
          ];
          const sql = `UPDATE rdw_entries SET ${updateFields}, updated_at = $${
            fields.length + 1
          } WHERE kenteken = $${fields.length + 2}`;
          await client.query(sql, params);
        }

        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      } finally {
        client.release();
      }

      if (onProgress) {
        onProgress(i + batch.length, toUpdate.length, "updating");
      }
    }
  }

  return {
    newCount: toInsert.length,
    updatedCount: toUpdate.length,
    addedKentekens,
  };
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
  bulkUpsert,
};
