const axios = require('axios');

const NOCO_API_BASE = 'https://your-nocodb-instance.com/api/v1/db/data/v1/your_project';
const API_TOKEN = 'your_api_token';

// Helper: Fetch all rows from a table with pagination
async function fetchAllRows(tableName) {
  let allRows = [];
  let offset = 0;
  const limit = 100;

  while (true) {
    const url = `${NOCO_API_BASE}/${tableName}?limit=${limit}&offset=${offset}`;
    const res = await axios.get(url, { headers: { 'xc-token': API_TOKEN } });
    allRows = allRows.concat(res.data.list);
    if (res.data.list.length < limit) break;
    offset += limit;
  }
  return allRows;
}

// Helper: Upsert a company record in GL101 table
async function upsertCompany(companyKey, data) {
  try {
    // NocoDB upsert: try PATCH (update) first, then POST (create)
    const patchUrl = `${NOCO_API_BASE}/GL101/${encodeURIComponent(companyKey)}`;
    await axios.patch(patchUrl, data, { headers: { 'xc-token': API_TOKEN } });
    console.log(`Updated company ${companyKey}`);
  } catch (patchErr) {
    if (patchErr.response && patchErr.response.status === 404) {
      // Not found, create new record with Company Key included
      try {
        const postData = { 'Company Key': companyKey, ...data };
        await axios.post(`${NOCO_API_BASE}/GL101`, postData, { headers: { 'xc-token': API_TOKEN } });
        console.log(`Created company ${companyKey}`);
      } catch (postErr) {
        console.error(`Create failed for company ${companyKey}`, postErr.response?.data || postErr.message);
      }
    } else {
      console.error(`Update failed for company ${companyKey}`, patchErr.response?.data || patchErr.message);
    }
  }
}

// Main function to sync
async function syncGhostScore() {
  console.log('Starting GhostScore sync', new Date().toISOString());

  // Fetch all reports from GL701
  const reports = await fetchAllRows('GL701');
  console.log(`Fetched ${reports.length} reports from GL701`);

  // Aggregate data by company
  const summary = {};

  for (const r of reports) {
    const companyKey = (r['Title'] || '').toLowerCase().replace(/\s+/g, '-'); // Create normalized key from Title or other field
    if (!companyKey) continue;

    if (!summary[companyKey]) {
      summary[companyKey] = {
        companyName: r['Title'] || '',
        reportsCount: 0,
        sumIncrements: 0,
        firstReportDate: r['Created At'] || null,
        lastReportDate: r['Created At'] || null,
        // Initialize percentages and counters
        noResponseCount: 0,
        ghostedAfterAssignmentCount: 0,
        ghostedAfterInterviewCount: 0,
        ghostedAfterOfferCount: 0,
        assignmentsGivenCount: 0,
        unpaidAssignmentCount: 0,
        confirmedReceiptCount: 0,
        feedbackReceivedCount: 0,
        noFeedbackCount: 0,
        followUpNoResponseCount: 0,
        officialRejectionCount: 0,
        aiScreeningRequiredCount: 0,
        wouldApplyAgainCount: 0,
        wouldRecommendCount: 0,
        impactList: [],
        roleCounts: {},
        recruiterCounts: {},
        assignmentTypeCounts: {},
        locationCounts: {},
        qualityFlags: [],
      };
    }

    const comp = summary[companyKey];
    comp.reportsCount++;

    // Example increments (you can define your own logic)
    // For demonstration: assuming each report adds 1 increment to sumIncrements
    comp.sumIncrements++;

    // Dates
    const reportDate = new Date(r['Created At']);
    if (!comp.firstReportDate || reportDate < new Date(comp.firstReportDate)) comp.firstReportDate = r['Created At'];
    if (!comp.lastReportDate || reportDate > new Date(comp.lastReportDate)) comp.lastReportDate = r['Created At'];

    // Example: count how many had No Response (assuming field 'Did You Receive An Official Rejection?' with values yes/no)
    if (r['Did You Receive an Official Rejection?'] === 'No Response') comp.noResponseCount++;
    if (r['Where in the Hiring Process Did Ghosting Happen?'] === 'After Assignment') comp.ghostedAfterAssignmentCount++;
    if (r['Where in the Hiring Process Did Ghosting Happen?'] === 'After Interview') comp.ghostedAfterInterviewCount++;
    if (r['Where in the Hiring Process Did Ghosting Happen?'] === 'After Offer') comp.ghostedAfterOfferCount++;

    // Track other counts similarly from single selects or checkboxes
    if (r['Did They Confirm Receipt of Your Work?'] === 'Yes') comp.confirmedReceiptCount++;
    if (r['Was the interview assignment paid?'] === 'No') comp.unpaidAssignmentCount++;
    if (r['Did You Receive Any Feedback on Your Work?'] === 'Yes') comp.feedbackReceivedCount++;
    if (r['Did You Receive Any Feedback on Your Work?'] === 'No') comp.noFeedbackCount++;
    if (r['Did You Follow Up After They Stopped Responding?'] === 'Yes') comp.followUpNoResponseCount++;
    if (r['AI Screening Required?'] === 'Yes' || r['Did the Company Require AI Screening Before Any Interview?'] === 'Yes') comp.aiScreeningRequiredCount++;

    if (r['Would You Apply to This Company Again?'] === 'Yes') comp.wouldApplyAgainCount++;
    if (r['Would You Recommend This Employer?'] === 'Yes') comp.wouldRecommendCount++;

    // Collect impacts (multi select)
    if (r['How Did This Ghosting Experience Affect You?']) {
      comp.impactList.push(r['How Did This Ghosting Experience Affect You?']);
    }

    // Count roles
    const role = r['Job Role Applied For'];
    if (role) comp.roleCounts[role] = (comp.roleCounts[role] || 0) + 1;

    // Count recruiters
    const recruiter = r['Recruiter Name or Email (Optional)'];
    if (recruiter) comp.recruiterCounts[recruiter] = (comp.recruiterCounts[recruiter] || 0) + 1;

    // Count assignment types
    const assignmentType = r['What Type of Assignment Was Given?'];
    if (assignmentType) comp.assignmentTypeCounts[assignmentType] = (comp.assignmentTypeCounts[assignmentType] || 0) + 1;

    // Count locations
    const location = r['Company Location'];
    if (location) comp.locationCounts[location] = (comp.locationCounts[location] || 0) + 1;

    // Data Quality Flag: collect if any low evidence flags from reports or skip for now
  }

  // Now build upsert payloads for each company summary
  for (const [companyKey, comp] of Object.entries(summary)) {
    // Calculate percentages (avoid divide by zero)
    const count = comp.reportsCount || 1;

    const payload = {
      'Company Name': comp.companyName,
      'Reports Count': count,
      'Sum Increments': comp.sumIncrements,
      'Avg Report Increment': comp.sumIncrements / count,
      'First Report Date': comp.firstReportDate,
      'Last Report Date': comp.lastReportDate,
      'No Response %': (comp.noResponseCount / count) * 100,
      'Ghosted After Assignment %': (comp.ghostedAfterAssignmentCount / count) * 100,
      'Ghosted After Interview %': (comp.ghostedAfterInterviewCount / count) * 100,
      'Ghosted After Offer %': (comp.ghostedAfterOfferCount / count) * 100,
      'Assignments Given %': (comp.assignmentsGivenCount || 0) / count * 100,
      'Unpaid Assignment %': (comp.unpaidAssignmentCount / count) * 100,
      'Confirmed Receipt %': (comp.confirmedReceiptCount / count) * 100,
      'Feedback Received %': (comp.feedbackReceivedCount / count) * 100,
      'No Feedback %': (comp.noFeedbackCount / count) * 100,
      'Follow-up No Response %': (comp.followUpNoResponseCount / count) * 100,
      'Official Rejection %': (comp.officialRejectionCount / count) * 100,
      'AI Screening Required %': (comp.aiScreeningRequiredCount / count) * 100,
      'Would Apply Again %': (comp.wouldApplyAgainCount / count) * 100,
      'Would Recommend %': (comp.wouldRecommendCount / count) * 100,
      'Top 3 Roles': Object.entries(comp.roleCounts).sort((a, b) => b[1] - a[1]).slice(0, 3).map(e => e[0]).join(', '),
      'Top 3 Recruiters': Object.entries(comp.recruiterCounts).sort((a, b) => b[1] - a[1]).slice(0, 3).map(e => e[0]).join(', '),
      'Top 3 Assignment Types': Object.entries(comp.assignmentTypeCounts).sort((a, b) => b[1] - a[1]).slice(0, 3).map(e => e[0]).join(', '),
      'Top 3 Locations': Object.entries(comp.locationCounts).sort((a, b) => b[1] - a[1]).slice(0, 3).map(e => e[0]).join(', '),
      'Common Impact': comp.impactList.join('; '),
      'Data Quality Flag': 'ok', // or your logic here
      'Confidence Score': 0.8,   // example fixed score, replace with your calc
      'Notes': '',               // leave empty or fill
    };

    await upsertCompany(companyKey, payload);
  }

  console.log('Finished GhostScore sync', new Date().toISOString());
}

// Run the sync
syncGhostScore().catch(console.error);
