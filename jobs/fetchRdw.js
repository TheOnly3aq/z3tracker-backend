const axios = require("axios");
const cron = require("node-cron");
const pool = require("../db");
const DailyCount = require("../models/dailyCount");
const MonthlyCount = require("../models/monthlyCount");
const RdwEntry = require("../models/rdwEntry");
const DailyDifference = require("../models/dailyDifference");

const fetchRdwData = async () => {
  try {
    await pool.query("SELECT 1");
    console.log("Starting RDW data fetch...");

    const existingKentekenList = await RdwEntry.distinct("kenteken");

    const response = await axios.get(
      "https://opendata.rdw.nl/resource/m9d7-ebf2.json?$where=upper(merk)='BMW'%20AND%20upper(handelsbenaming)%20like%20'%25Z3%25'&$limit=4000"
    );

    const entries = response.data;
    console.log(`(Z3) Fetched ${entries.length} entries from RDW API`);

    const now = new Date();
    const dateString = now.toISOString().split("T")[0];
    const yearMonth = `${now.getFullYear()}-${String(
      now.getMonth() + 1
    ).padStart(2, "0")}`;

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

    let updatedCount = 0;
    let newCount = 0;
    const addedKentekens = [];

    for (const entry of entries) {
      try {
        const existingEntry = await RdwEntry.findOne({
          kenteken: entry.kenteken,
        });

        if (existingEntry) {
          await RdwEntry.findOneAndUpdate(
            { kenteken: entry.kenteken },
            {
              ...entry,
              lastUpdated: new Date(),
            }
          );
          updatedCount++;
        } else {
          await RdwEntry.create({
            ...entry,
            lastUpdated: new Date(),
          });
          newCount++;
          addedKentekens.push(entry.kenteken);
        }
      } catch (entryError) {
        console.warn(
          `(Z3) Skipped entry ${entry.kenteken}:`,
          entryError.message
        );
      }
    }

    console.log(`(Z3) Saved RDW count for ${dateString}: ${entries.length}`);
    console.log(`(Z3) Saved RDW count for ${yearMonth}: ${entries.length}`);
    console.log(
      `(Z3) Cached data: ${newCount} new entries, ${updatedCount} updated entries`
    );

    const currentKentekenList = entries.map((entry) => entry.kenteken);

    const removedKentekens = existingKentekenList.filter(
      (kenteken) => !currentKentekenList.includes(kenteken)
    );

    const deleteResult = await RdwEntry.deleteMany({
      kenteken: { $nin: currentKentekenList },
    });

    if (deleteResult.deletedCount > 0) {
      console.log(
        `(Z3) Removed ${deleteResult.deletedCount} entries that are no longer in the API`
      );
    }

    const totalChanges = addedKentekens.length + removedKentekens.length;

    if (totalChanges > 0) {
      await DailyDifference.findOneAndUpdate(
        { date: dateString },
        {
          $set: {
            added: addedKentekens,
            removed: removedKentekens,
            totalChanges: totalChanges,
          },
        },
        { upsert: true, new: true }
      );

      console.log(
        `(Z3) Daily differences for ${dateString}: ${addedKentekens.length} added, ${removedKentekens.length} removed`
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
  } catch (err) {
    console.error("(Z3) Error fetching RDW data:", err.message);
  }
};

const scheduleIS250CJob = () => {
  console.log("Scheduling IS250C cron job...");
  cron.schedule("0 0 * * *", fetchRdwData);
};

module.exports = { fetchRdwData, scheduleIS250CJob };
