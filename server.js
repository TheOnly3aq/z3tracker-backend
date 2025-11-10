require("dotenv").config();
const express = require("express");
const pool = require("./db");
const cors = require("cors");
const swaggerUi = require("swagger-ui-express");
const checkApiKey = require("./apiKeyMiddleware");
const statsRoute = require("./routes/stats");
const { scheduleIS250CJob } = require("./jobs/fetchRdw");
const RdwEntry = require("./models/rdwEntry");
const DailyCount = require("./models/dailyCount");
const MonthlyCount = require("./models/monthlyCount");
const DailyDifference = require("./models/dailyDifference");

const swaggerDocument = require("./swagger.json");

const app = express();
const PORT = process.env.PORT || 5050;
app.use(cors());
app.use(express.json());
app.use("/api/docs", swaggerUi.serve, swaggerUi.setup(swaggerDocument));

app.get("/robots.txt", function (req, res) {
  res.type("text/plain");
  res.send(
    `User-agent: *
Disallow: /`
  );
});

app.get("/swagger.json", (req, res) => {
  res.json(swaggerDocument);
});

const startServer = async () => {
  try {
    await pool.query("SELECT 1");
    console.log("PostgreSQL connected");

    await RdwEntry.createTable();
    await DailyCount.createTable();
    await MonthlyCount.createTable();
    await DailyDifference.createTable();

    scheduleIS250CJob();

    app.use("/api/:car/stats", checkApiKey, statsRoute);

    app.listen(PORT, () => console.log(`(!) Server running on port ${PORT}`));
  } catch (err) {
    console.error("Failed to start server:", err);
    process.exit(1);
  }
};

startServer();
