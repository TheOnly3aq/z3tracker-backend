const pool = require("../db");
const DailyDifference = require("../models/dailyDifference");
const testData = require("../test-daily-differences.json");

const importTestData = async () => {
  try {
    await pool.query("SELECT 1");
    console.log("Connected to PostgreSQL");

    await DailyDifference.createTable();

    await DailyDifference.deleteMany({});
    console.log("Cleared existing daily differences");

    const result = await DailyDifference.insertMany(testData);
    console.log(`Inserted ${result.length} daily difference records`);

    console.log("Test data import completed successfully!");
  } catch (error) {
    console.error("Error importing test data:", error);
  } finally {
    await pool.end();
    console.log("Disconnected from PostgreSQL");
  }
};

importTestData();
