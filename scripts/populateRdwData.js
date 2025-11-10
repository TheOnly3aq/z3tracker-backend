const pool = require("../db");
const { fetchRdwData } = require("../jobs/fetchRdw");
const RdwEntry = require("../models/rdwEntry");
const DailyCount = require("../models/dailyCount");
const MonthlyCount = require("../models/monthlyCount");
const DailyDifference = require("../models/dailyDifference");

const runPopulation = async () => {
  try {
    await pool.query("SELECT 1");
    console.log("Connected to PostgreSQL");

    await RdwEntry.createTable();
    await DailyCount.createTable();
    await MonthlyCount.createTable();
    await DailyDifference.createTable();

    console.log("Starting initial RDW data population...");
    await fetchRdwData();

    console.log("Data population completed!");
    process.exit(0);
  } catch (error) {
    console.error("Error during population:", error);
    process.exit(1);
  }
};

runPopulation();
