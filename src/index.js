import axios from 'axios';

const NOCO_API_BASE = process.env.NOCODB_URL;
const API_TOKEN = process.env.NOCODB_API_KEY;
const PAGE_SIZE = parseInt(process.env.PAGE_SIZE) || 100;
const SOURCE_TABLE = process.env.SOURCE_TABLE;
const TARGET_TABLE = process.env.TARGET_TABLE;

// Axios instance with API token header
const api = axios.create({
  baseURL: NOCO_API_BASE,
  headers: {
    'xc-auth': API_TOKEN,
    'Content-Type': 'application/json',
  },
});

async function fetchAllRows(tableName) {
  let rows = [];
  let offset = 0;

  while (true) {
    try {
      const res = await api.get(`/table/${encodeURIComponent(tableName)}`, {
        params: {
          limit: PAGE_SIZE,
          offset,
        },
      });
      const data = res.data.list || res.data;
      if (!data.length) break;

      rows = rows.concat(data);
      if (data.length < PAGE_SIZE) break; // last page reached
      offset += PAGE_SIZE;
    } catch (error) {
      console.error(`Error fetching rows from ${tableName}:`, error.response?.data || error.message);
      break;
    }
  }
  return rows;
}

// Helper: normalize company key from company name (lowercase, no spaces/special chars)
function normalizeCompanyKey(name) {
  return name?.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9\-]/g, '') || '';
}

function aggregateReports(rows) {
  // Example aggregation: count total reports per company
  const companyMap = new Map();

  rows.forEach(row => {
    const companyName = row['Title'] || row['Company Name'] || '';
    const companyKey = normalizeCompanyKey(companyName);

    if (!companyKey) return;

    if (!companyMap.has(companyKey)) {
      companyMap.set(companyKey, {
        companyKey,
        companyName,
        reportsCount: 0,
        sumIncrements: 0,
        // initialize other aggregated fields here as needed
        firstReportDate: null,
        lastReportDate: null,
        ghostScore: 0,
        // Add defaults for all numeric % fields too
        noResponsePct: 0,
        ghostedAfterAssignmentPct: 0,
        ghostedAfterInterviewPct: 0,
        ghostedAfterOfferPct: 0,
        assignmentsGivenPct: 0,
        unpaidAssignmentPct: 0,
        confirmedReceiptPct: 0,
        feedbackReceivedPct: 0,
        noFeedbackPct: 0,
        followUpNoResponsePct: 0,
        officialRejectionPct: 0,
        aiScreeningRequiredPct: 0,
        wouldApplyAgainPct: 0,
        wouldRecommendPct: 0,
        top3Roles: new Map(),
        top3Recruiters: new Map(),
        top3AssignmentTypes: new Map(),
        top3Locations: new Map(),
        commonImpact: new Map(),
        dataQualityFlag: 'ok',
        confidenceScore: 1,
        notes: '',
      });
    }

    const company = companyMap.get(companyKey);

    company.reportsCount += 1;
    // Example: increment sumIncrements if there's a field "Increment" or similar
    if (row['Increment']) {
      company.sumIncrements += Number(row['Increment']) || 0;
    }

    // Track first and last report date
    const reportDateStr = row['First Report Date'] || row['Last Report Date'] || row['Date'] || null;
    if (reportDateStr) {
      const reportDate = new Date(reportDateStr);
      if (!company.firstReportDate || reportDate < company.firstReportDate) {
        company.firstReportDate = reportDate;
      }
      if (!company.lastReportDate || reportDate > company.lastReportDate) {
        company.lastReportDate = reportDate;
      }
    }

    // Increment count of job roles, recruiters, assignment types, locations, impacts for top 3 calculation
    const role = row['Job Role Applied For'] || null;
    if (role) company.top3Roles.set(role, (company.top3Roles.get(role) || 0) + 1);

    const recruiter = row['Recruiter Name or Email (Optional)'] || null;
    if (recruiter) company.top3Recruiters.set(recruiter, (company.top3Recruiters.get(recruiter) || 0) + 1);

    const assignmentType = row['What Type of Assignment Was Given?'] || null;
    if (assignmentType) company.top3AssignmentTypes.set(assignmentType, (company.top3AssignmentTypes.get(assignmentType) || 0) + 1);

    const location = row['Company Location'] || null;
    if (location) company.top3Locations.set(location, (company.top3Locations.get(location) || 0) + 1);

    const impacts = row['How Did This Ghosting Experience Affect You?'] || '';
    if (impacts) {
      // multi-select, split by commas
      const impactList = impacts.split(',').map(i => i.trim());
      impactList.forEach(imp => {
        if (imp) company.commonImpact.set(imp, (company.commonImpact.get(imp) || 0) + 1);
      });
    }

    // TODO: Calculate percentages (noResponsePct etc.) based on relevant fields from source row

    // This aggregation is an example, add more precise logic depending on your data fields
  });

  // After aggregation, format top 3s and impacts as comma-separated strings, calculate avg report increment etc.
  const result = [];
  for (const company of companyMap.values()) {
    const top3Roles = [...company.top3Roles.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(e => e[0])
      .join(', ');

    const top3Recruiters = [...company.top3Recruiters.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(e => e[0])
      .join(', ');

    const top3AssignmentTypes = [...company.top3AssignmentTypes.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(e => e[0])
      .join(', ');

    const top3Locations = [...company.top3Locations.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(e => e[0])
      .join(', ');

    const commonImpact = [...company.commonImpact.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(e => e[0])
      .join(', ');

    const avgReportIncrement = company.reportsCount > 0 ? company.sumIncrements / company.reportsCount : 0;

    result.push({
      'Company Key': company.companyKey,
      'Company Name': company.companyName,
      'Reports Count': company.reportsCount,
      'Sum Increments': company.sumIncrements,
      'Avg Report Increment': avgReportIncrement,
      'GhostScore': company.ghostScore,
      'First Report Date': company.firstReportDate ? company.firstReportDate.toISOString().split('T')[0] : null,
      'Last Report Date': company.lastReportDate ? company.lastReportDate.toISOString().split('T')[0] : null,
      'No Response %': company.noResponsePct,
      'Ghosted After Assignment %': company.ghostedAfterAssignmentPct,
      'Ghosted After Interview %': company.ghostedAfterInterviewPct,
      'Ghosted After Offer %': company.ghostedAfterOfferPct,
      'Assignments Given %': company.assignmentsGivenPct,
      'Unpaid Assignment %': company.unpaidAssignmentPct,
      'Confirmed Receipt %': company.confirmedReceiptPct,
      'Feedback Received %': company.feedbackReceivedPct,
      'No Feedback %': company.noFeedbackPct,
      'Follow-up No Response %': company.followUpNoResponsePct,
      'Official Rejection %': company.officialRejectionPct,
      'AI Screening Required %': company.aiScreeningRequiredPct,
      'Would Apply Again %': company.wouldApplyAgainPct,
      'Would Recommend %': company.wouldRecommendPct,
      'Top 3 Roles': top3Roles,
      'Top 3 Recruiters': top3Recruiters,
      'Top 3 Assignment Types': top3AssignmentTypes,
      'Top 3 Locations': top3Locations,
      'Common Impact': commonImpact,
      'Data Quality Flag': company.dataQualityFlag,
      'Confidence Score': company.confidenceScore,
      'Notes': company.notes,
    });
  }

  return result;
}

async function upsertRow(row) {
  try {
    // Use upsert endpoint (NocoDB v0.92+ supports UPSERT by primary key)
    // Assuming "Company Key" is unique key for upsert
    await api.post(`/table/${encodeURIComponent(TARGET_TABLE)}/upsert`, {
      record: row,
      key: 'Company Key',
    });
    console.log(`Upserted company ${row['Company Key']}`);
  } catch (error) {
    console.error(`Create failed:`, error.response?.data || error.message);
  }
}

async function main() {
  console.log(`Starting GhostScore sync ${new Date().toISOString()}`);

  const sourceRows = await fetchAllRows(SOURCE_TABLE);
  console.log(`Fetched ${sourceRows.length} rows from ${SOURCE_TABLE}`);

  const aggregated = aggregateReports(sourceRows);
  console.log(`Aggregated into ${aggregated.length} company records`);

  for (const companyRow of aggregated) {
    await upsertRow(companyRow);
  }

  console.log(`Finished GhostScore sync ${new Date().toISOString()}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
