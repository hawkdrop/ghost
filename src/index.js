import axios from 'axios';

const NOCODB_API_KEY = process.env.NOCODB_API_KEY;
const NOCODB_URL = process.env.NOCODB_URL;
const PAGE_SIZE = Number(process.env.PAGE_SIZE) || 100;
const SOURCE_TABLE = process.env.SOURCE_TABLE; // GL701 List name
const TARGET_TABLE = process.env.TARGET_TABLE; // GL101 List name

const headers = {
  'xc-token': NOCODB_API_KEY,
  'Content-Type': 'application/json',
};

async function fetchRows(table, offset = 0) {
  const url = `${NOCODB_URL}/api/v1/db/data/v1/${table}?limit=${PAGE_SIZE}&offset=${offset}`;
  const res = await axios.get(url, { headers });
  return res.data;
}

function normalizeCompanyKey(name) {
  return name.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9\-]/g, '');
}

function transformSourceToTarget(src) {
  return {
    'Company Key': normalizeCompanyKey(src['Company Website URL (Optional)'] || src['Title'] || ''),
    'Company Name': src['Title'] || '',
    'Reports Count': 1,
    'Sum Increments': 0,
    'Avg Report Increment': 0,
    'GhostScore': 0,
    'First Report Date': src['Created At'] || null,
    'Last Report Date': src['Created At'] || null,
    'No Response %': 0,
    'Ghosted After Assignment %': 0,
    'Ghosted After Interview %': 0,
    'Ghosted After Offer %': 0,
    'Assignments Given %': 0,
    'Unpaid Assignment %': 0,
    'Confirmed Receipt %': 0,
    'Feedback Received %': 0,
    'No Feedback %': 0,
    'Follow-up No Response %': 0,
    'Official Rejection %': 0,
    'AI Screening Required %': 0,
    'Would Apply Again %': 0,
    'Would Recommend %': 0,
    'Top 3 Roles': '',
    'Top 3 Recruiters': '',
    'Top 3 Assignment Types': '',
    'Top 3 Locations': '',
    'Common Impact': '',
    'Data Quality Flag': 'ok',
    'Confidence Score': 0,
    'Notes': '',
  };
}

async function upsertRow(table, data) {
  try {
    // Check if record exists by Company Key
    const filterUrl = `${NOCODB_URL}/api/v1/db/data/v1/${table}?filter={"Company Key":{"_eq":"${data['Company Key']}"}}`;
    const existing = await axios.get(filterUrl, { headers });
    if (existing.data && existing.data.length > 0) {
      // Update existing record
      const id = existing.data[0].id;
      await axios.patch(`${NOCODB_URL}/api/v1/db/data/v1/${table}/${id}`, data, { headers });
    } else {
      // Create new record
      await axios.post(`${NOCODB_URL}/api/v1/db/data/v1/${table}`, data, { headers });
    }
  } catch (error) {
    console.error(`Error upserting company ${data['Company Key']}:`, error.response?.data || error.message);
  }
}

async function main() {
  let offset = 0;
  let rowsFetched;
  do {
    const data = await fetchRows(SOURCE_TABLE, offset);
    rowsFetched = data.length;
    for (const srcRow of data) {
      const targetData = transformSourceToTarget(srcRow);
      await upsertRow(TARGET_TABLE, targetData);
    }
    offset += rowsFetched;
  } while (rowsFetched === PAGE_SIZE);
  console.log('Done');
}

main();
