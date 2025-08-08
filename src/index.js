// src/index.js
import axios from "axios";
import dotenv from "dotenv";

dotenv.config();

const {
  NOCODB_URL,
  NOCODB_API_KEY,
  SOURCE_TABLE,
  TARGET_TABLE,
  PAGE_SIZE = 200,
  DRY_RUN = "true",
  RATE_LIMIT_MS = 150,
} = process.env;

if (!NOCODB_URL || !NOCODB_API_KEY || !SOURCE_TABLE || !TARGET_TABLE) {
  console.error("❌ Missing environment variables. Please set NOCODB_URL, NOCODB_API_KEY, SOURCE_TABLE, TARGET_TABLE.");
  process.exit(1);
}

const api = axios.create({
  baseURL: NOCODB_URL.replace(/\/$/, ""), // remove trailing slash
  headers: {
    "xc-token": NOCODB_API_KEY,
    "Content-Type": "application/json",
  },
});

// Fetch all rows from a NocoDB table
async function fetchAllRows(tableId) {
  console.log(`📥 Fetching rows from table ${tableId}...`);
  let rows = [];
  let offset = 0;

  while (true) {
    const res = await api.get(`/api/v2/tables/${tableId}/records`, {
      params: {
        limit: PAGE_SIZE,
        offset,
      },
    });
    if (res.status !== 200) {
      throw new Error(`Failed to fetch rows: ${res.status} ${res.statusText}`);
    }
    const data = res.data.list || [];
    rows = rows.concat(data);
    if (data.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }
  console.log(`✅ Fetched ${rows.length} rows from table ${tableId}`);
  return rows;
}

// Insert rows into NocoDB target table
async function insertRows(tableId, rows) {
  console.log(`📤 Inserting ${rows.length} rows into table ${tableId}...`);
  for (const row of rows) {
    if (DRY_RUN === "true") {
      console.log("🛑 DRY_RUN mode — would insert:", row);
      continue;
    }
    await api.post(`/api/v2/tables/${tableId}/records`, row);
    await new Promise(r => setTimeout(r, RATE_LIMIT_MS));
  }
  console.log("✅ Insert complete.");
}

// Example GhostScore calculation logic
function calculateGhostScore(record) {
  let score = 500; // base score
  let answers = {};

  // Example scoring logic — replace with your full Q&A
  if (record.question1 === "Yes") {
    score += 50;
    answers.question1 = "Yes";
  } else {
    score -= 20;
    answers.question1 = "No";
  }

  if (record.question2 && record.question2 > 5) {
    score += 100;
    answers.question2 = record.question2;
  }

  // More Q&A scoring here...
  // Keep ALL your original conditions

  return { score, answers };
}

async function main() {
  try {
    console.log("🚀 Starting GhostScore sync", new Date().toISOString());

    const sourceRows = await fetchAllRows(SOURCE_TABLE);

    const scoredRows = sourceRows.map(row => {
      const { score, answers } = calculateGhostScore(row);
      return {
        ...answers,
        ghostScore: score,
      };
    });

    await insertRows(TARGET_TABLE, scoredRows);

    console.log("🎯 GhostScore sync complete.");
  } catch (err) {
    console.error("❌ Fatal error:", err.message);
    process.exit(1);
  }
}

main();
