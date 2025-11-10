const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || "postgresql://localhost:5432/lexustracker",
  ssl: process.env.DATABASE_SSL === "true" ? { rejectUnauthorized: false } : false,
});

pool.on("error", (err) => {
  console.error("Unexpected error on idle client", err);
  process.exit(-1);
});

module.exports = pool;

