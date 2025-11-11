const axios = require("axios");
const cron = require("node-cron");
const pool = require("../db");
const DailyCount = require("../models/dailyCount");
const MonthlyCount = require("../models/monthlyCount");
const RdwEntry = require("../models/rdwEntry");
const DailyDifference = require("../models/dailyDifference");

const fetchRdwData = async () => {
  const startTime = Date.now();
  try {
    await pool.query("SELECT 1");
    console.log("(Z3) Starting RDW data fetch...");

    console.log("(Z3) Loading existing kentekens from database...");
    const existingKentekenList = await RdwEntry.distinct("kenteken");
    console.log(
      `(Z3) Found ${existingKentekenList.length} existing entries in database`
    );

    console.log("(Z3) Fetching data from RDW API...");
    const response = await axios.get(
      "https://opendata.rdw.nl/resource/m9d7-ebf2.json?$where=upper(merk)='BMW'%20AND%20(upper(handelsbenaming)%20like%20'%25Z3%25'%20OR%20upper(handelsbenaming)%20like%20'%25Z%20REIHE%25')&$limit=15000"
    );

    const entries = response.data;
    console.log(`(Z3) Fetched ${entries.length} entries from RDW API`);

    const now = new Date();
    const dateString = now.toISOString().split("T")[0];
    const yearMonth = `${now.getFullYear()}-${String(
      now.getMonth() + 1
    ).padStart(2, "0")}`;

    console.log("(Z3) Updating daily and monthly counts...");
    await DailyCount.findOneAndUpdate(
      { date: dateString },
      { $set: { count: entries.length } },
      { upsert: true, new: true }
    );

    await MonthlyCount.findOneAndUpdate(
      { month: yearMonth },
      { $set: { count: entries.length } },
      { upsert: true, new: true }
    );

    console.log("(Z3) Processing entries (this may take a moment)...");
    let lastProgressTime = Date.now();
    let lastProgressCount = 0;

    const progressCallback = (current, total, operation) => {
      const now = Date.now();
      if (now - lastProgressTime > 1000 || current === total) {
        const rate = (
          (current - lastProgressCount) /
          ((now - lastProgressTime) / 1000)
        ).toFixed(0);
        console.log(
          `(Z3) ${operation}: ${current}/${total} (${(
            (current / total) *
            100
          ).toFixed(1)}%) - ${rate} entries/sec`
        );
        lastProgressTime = now;
        lastProgressCount = current;
      }
    };

    const result = await RdwEntry.bulkUpsert(entries, progressCallback);

    console.log(
      `(Z3) Processing complete: ${result.newCount} new, ${result.updatedCount} updated`
    );
    console.log(`(Z3) Saved RDW count for ${dateString}: ${entries.length}`);
    console.log(`(Z3) Saved RDW count for ${yearMonth}: ${entries.length}`);

    const currentKentekenList = entries.map((entry) => entry.kenteken);
    const removedKentekens = existingKentekenList.filter(
      (kenteken) => !currentKentekenList.includes(kenteken)
    );

    if (removedKentekens.length > 0) {
      console.log(
        `(Z3) Removing ${removedKentekens.length} entries that are no longer in the API...`
      );
      const deleteResult = await RdwEntry.deleteMany({
        kenteken: { $nin: currentKentekenList },
      });
      console.log(`(Z3) Removed ${deleteResult.deletedCount} entries`);
    }

    const totalChanges = result.addedKentekens.length + removedKentekens.length;

    if (totalChanges > 0) {
      await DailyDifference.findOneAndUpdate(
        { date: dateString },
        {
          $set: {
            added: result.addedKentekens,
            removed: removedKentekens,
            totalChanges: totalChanges,
          },
        },
        { upsert: true, new: true }
      );

      console.log(
        `(Z3) Daily differences for ${dateString}: ${result.addedKentekens.length} added, ${removedKentekens.length} removed`
      );
    } else {
      await DailyDifference.findOneAndUpdate(
        { date: dateString },
        {
          $set: {
            added: [],
            removed: [],
            totalChanges: 0,
          },
        },
        { upsert: true, new: true }
      );

      console.log(`(Z3) No changes detected for ${dateString}`);
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`(Z3) Data fetch completed in ${duration}s`);
  } catch (err) {
    console.error("(Z3) Error fetching RDW data:", err.message);
    throw err;
  }
};

fetchRdwData()

const scheduleZ3Job = () => {
  console.log("Scheduling Z3 cron job...");
  cron.schedule("0 0 * * *", fetchRdwData);
};

module.exports = { fetchRdwData, scheduleZ3Job };
