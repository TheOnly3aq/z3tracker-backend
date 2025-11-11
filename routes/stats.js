const express = require("express");
const router = express.Router({ mergeParams: true });
const DailyCount = require("../models/dailyCount");
const MonthlyCount = require("../models/monthlyCount");
const RdwEntry = require("../models/rdwEntry");
const DailyDifference = require("../models/dailyDifference");
const DailyStats = require("../models/dailyStats");

const getModelsForCar = (car) => {
  const key = (car || "").toUpperCase();
  if (key === "Z3") {
    return {
      DailyCount,
      MonthlyCount,
      RdwEntry,
      DailyDifference,
    };
  }
  return null;
};

router.get("/summary", async (req, res) => {
  try {
    const dateString = new Date().toISOString().split("T")[0];
    const stats = await DailyStats.findOne({ date: dateString });

    if (!stats) {
      return res.status(404).json({ error: "Stats not found for today" });
    }

    const colorCounts = typeof stats.color_counts === "string" 
      ? JSON.parse(stats.color_counts) 
      : stats.color_counts;

    res.json({
      date: stats.date,
      totalVehicles: stats.total_vehicles,
      insuredCount: stats.insured_count,
      importedCount: stats.imported_count,
      colors: colorCounts,
    });
  } catch (err) {
    console.error("Error fetching summary stats:", err);
    res.status(500).json({ error: "Failed to fetch summary stats" });
  }
});

router.use((req, res, next) => {
  const models = getModelsForCar(req.params.car);
  if (!models) {
    return res.status(400).json({
      error: "Unsupported car segment. Use Z3.",
    });
  }
  req.models = models;
  next();
});

router.get("/daily-count", async (req, res) => {
  try {
    const result = await req.models.DailyCount.find();
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch daily stats" });
  }
});

router.get("/monthly-count", async (req, res) => {
  try {
    const result = await req.models.MonthlyCount.find();
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch monthly stats" });
  }
});

router.get("/rdw-data", async (req, res) => {
  try {
    res.set({
      "Cache-Control": "public, max-age=3600",
      ETag: `"rdw-${Date.now()}"`,
    });

    const { search = "", sortBy = "kenteken", sortOrder = "asc" } = req.query;

    let query = {};
    if (search) {
      query.$or = [
        { kenteken: { $regex: search, $options: "i" } },
        { merk: { $regex: search, $options: "i" } },
        { handelsbenaming: { $regex: search, $options: "i" } },
        { eerste_kleur: { $regex: search, $options: "i" } },
        { inrichting: { $regex: search, $options: "i" } },
      ];
    }

    const result = await req.models.RdwEntry.find(query);
    let rows = result.rows;

    const sortField = sortBy === "kenteken" ? "kenteken" : sortBy;
    rows.sort((a, b) => {
      const aVal = a[sortField] || "";
      const bVal = b[sortField] || "";
      if (sortOrder === "desc") {
        return bVal.localeCompare(aVal);
      }
      return aVal.localeCompare(bVal);
    });

    const allEntriesResult = await req.models.RdwEntry.find({});
    const sorted = allEntriesResult.rows.sort((a, b) => {
      const aDate = new Date(a.last_updated || 0);
      const bDate = new Date(b.last_updated || 0);
      return bDate - aDate;
    });
    const lastUpdate = sorted[0]?.last_updated || null;

    res.json({
      data: rows,
      lastUpdated: lastUpdate,
      count: rows.length,
    });
  } catch (err) {
    console.error("Error fetching RDW data:", err);
    res.status(500).json({ error: "Failed to fetch RDW data" });
  }
});

router.get("/rdw-data/:kenteken", async (req, res) => {
  try {
    const { kenteken } = req.params;

    res.set({
      "Cache-Control": "public, max-age=3600",
    });

    const entry = await req.models.RdwEntry.findOne({
      kenteken: kenteken.toUpperCase(),
    });

    if (!entry) {
      return res.status(404).json({ error: "Vehicle not found" });
    }

    res.json(entry);
  } catch (err) {
    console.error("Error fetching RDW entry:", err);
    res.status(500).json({ error: "Failed to fetch vehicle data" });
  }
});

router.get("/rdw-stats", async (req, res) => {
  try {
    res.set({
      "Cache-Control": "public, max-age=1800",
    });

    const totalCount = await req.models.RdwEntry.countDocuments();
    
    const allEntriesResult = await req.models.RdwEntry.find({});
    const sortedByDate = allEntriesResult.rows.sort((a, b) => {
      const aDate = new Date(a.last_updated || 0);
      const bDate = new Date(b.last_updated || 0);
      return bDate - aDate;
    });
    const lastUpdate = sortedByDate[0]?.last_updated || null;

    const colorStats = await req.models.RdwEntry.aggregate([
      { $group: { _id: "$eerste_kleur", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 10 },
    ]);

    const yearStats = await req.models.RdwEntry.aggregate([
      {
        $addFields: {
          year: {
            $substr: ["$datum_eerste_toelating", 0, 4],
          },
        },
      },
      { $group: { _id: "$year", count: { $sum: 1 } } },
      { $sort: { _id: 1 } },
    ]);

    const inrichtingStats = await req.models.RdwEntry.aggregate([
      { $group: { _id: "$inrichting", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
    ]);

    res.json({
      totalCount,
      lastUpdated: lastUpdate,
      statistics: {
        colorDistribution: colorStats,
        yearDistribution: yearStats,
        bodyTypeDistribution: inrichtingStats,
      },
    });
  } catch (err) {
    console.error("Error fetching RDW statistics:", err);
    res.status(500).json({ error: "Failed to fetch RDW statistics" });
  }
});

router.get("/daily-differences", async (req, res) => {
  try {
    res.set({
      "Cache-Control": "public, max-age=1800",
    });

    const { date, limit = 30 } = req.query;

    let query = {};
    if (date) {
      query.date = date;
    }

    const result = await req.models.DailyDifference.find(query, { limit: parseInt(limit) });
    const rows = result.rows;

    const transformedData = rows.map((entry) => ({
      date: entry.date,
      changes:
        entry.total_changes > 0
          ? {
              added: entry.added || [],
              removed: entry.removed || [],
            }
          : [],
      totalChanges: entry.total_changes,
      createdAt: entry.created_at,
      updatedAt: entry.updated_at,
    }));

    res.json({
      data: transformedData,
      count: rows.length,
    });
  } catch (err) {
    console.error("Error fetching daily differences:", err);
    res.status(500).json({ error: "Failed to fetch daily differences" });
  }
});

module.exports = router;
