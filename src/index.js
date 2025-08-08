import axios from "axios";
import dotenv from "dotenv";
import dayjs from "dayjs";
dotenv.config();

const {
  NOCODB_URL,
  NOCODB_API_KEY,
  SOURCE_TABLE = "GL701",
  TARGET_TABLE = "GL101",
  PAGE_SIZE = 200,
  DRY_RUN = "true",
  RATE_LIMIT_MS = 150,
} = process.env;

if(!NOCODB_URL||!NOCODB_API_KEY){console.error("Error: NOCODB_URL and NOCODB_API_KEY must be set");process.exit(1);}
const axiosInstance=axios.create({
  baseURL:NOCODB_URL.replace(/\/$/,""),
  headers:{"xc-token":NOCODB_API_KEY,"Content-Type":"application/json"},
  timeout:120000
});

function sleep(ms){return new Promise(r=>setTimeout(r,ms));}
function normalizeCompanyKey(name){
  if(!name)return"unknown";
  return name.toString().normalize("NFKD").replace(/[\u0300-\u036f]/g,"")
    .replace(/\b(ltd|pvt|private|inc|llc|co|company|ltd\.)\b/gi,"")
    .replace(/[^a-z0-9]+/gi," ").trim().toLowerCase().replace(/\s+/g,"-");
}
function safeGet(row,key){
  if(!row)return"";
  if(row[key]!==undefined&&row[key]!==null)return row[key];
  if(row.fields&&row.fields[key]!==undefined&&row.fields[key]!==null)return row.fields[key];
  const all={...(row.fields||{}),...row};
  const found=Object.keys(all).find(k=>k.toLowerCase()===key.toLowerCase());
  return found?all[found]:"";
}
function maskRecruiter(name){
  if(!name)return"";
  const s=name.toString();
  return s.length<=4?s:s.slice(0,4)+"****";
}
function topNFromCounts(counts,n=3){
  return Object.entries(counts).filter(([k])=>k&&k!=="")
    .sort((a,b)=>b[1]-a[1]).slice(0,n).map(([k])=>k);
}
function getRowId(row){
  return row?.id??row?.ID??row?.insertId??row?.rowid??row?._id??row?.row_id??null;
}
function pct(count,total){
  if(!total)return 0;
  return Math.round((count/total)*1000)/10;
}
function calculateConfidence(N){
  if(!N)return 0;
  const val=Math.log10(N+1)/Math.log10(11);
  return Math.min(1,Math.max(0,val));
}
function clamp(n,a=0,b=999){
  return Math.max(a,Math.min(b,n));
}

const STAGE_WEIGHTS={
  "No Response After Application":5,
  "No Response After Initial Inquiry":5,
  "Ghosted After First Interview":20,
  "Ghosted After Multiple Interviews":50,
  "Ghosted After Completing an Assignment":25,
  "Ghosted After Verbal Offer":60,
};

function reportIncrement(r){
  let inc=0;
  const stage=safeGet(r,"Where in the Hiring Process Did Ghosting Happen?");
  inc+=STAGE_WEIGHTS[stage]||0;
  const assignment=safeGet(r,"What Type of Assignment Was Given?");
  const assignmentProvided=assignment&&assignment.toString().trim()&&!/no assignments required/i.test(assignment);
  if(assignmentProvided){
    inc+=25;
    const dur=(safeGet(r,"How Long Did It Take You to Complete the Assignment?")||"").toString();
    if(/<\s*2/i.test(dur)||/less than 2/i.test(dur))inc+=20;
    else if(/2.?[\-–—]?5/i.test(dur)||/2\s*–\s*5/i.test(dur))inc+=30;
    else if(/5.?[\-–—]?10/i.test(dur)||/5\s*–\s*10/i.test(dur))inc+=50;
    else if(/more than 10|> ?10/i.test(dur)||/10\+/.test(dur))inc+=75;
  }
  const paid=(safeGet(r,"Was the interview assignment paid?")||"").toString();
  if(/^\s*no\b/i.test(paid))inc+=30;
  const feedback=(safeGet(r,"Did You Receive Any Feedback on Your Work?")||"").toString();
  if(/no[, ]*no feedback/i.test(feedback)||/no feedback/i.test(feedback))inc+=30;
  else if(/vague/i.test(feedback))inc+=10;
  const receipt=(safeGet(r,"Did They Confirm Receipt of Your Work?")||"").toString();
  if(/^\s*no\b/i.test(receipt))inc+=10;
  const followup=(safeGet(r,"Did You Follow Up After They Stopped Responding?")||"").toString();
  if(/no response/i.test(followup))inc+=15;
  else if(/vague excuse/i.test(followup)||/vague/i.test(followup))inc+=8;
  const wait=(safeGet(r,"How Long Did You Wait Before Realizing You Were Ghosted?")||"").toString();
  if(/<\s*1/i.test(wait)||/less than 1/i.test(wait)||/<\s*1 Week/i.test(wait))inc+=15;
  else if(/1.?[\-–—]?2/i.test(wait)||/1\s*–\s*2/i.test(wait))inc+=25;
  else if(/2.?[\-–—]?4/i.test(wait)||/2\s*–\s*4/i.test(wait))inc+=40;
  else if(/more than 1 month|> ?1 month/i.test(wait)||/more than 1 month/i.test(wait))inc+=60;
  const rej=(safeGet(r,"Did You Receive an Official Rejection?")||"").toString();
  if(/no[, ]*complete silence/i.test(rej)||/no, complete silence/i.test(rej)||/^\s*no\b/i.test(rej))inc+=40;
  return inc;
}

async function fetchAllRows(tableName){
  const limit=Number(PAGE_SIZE)||200;
  let offset=0;
  let collected=[];
  while(true){
    const url=`/api/v2/tables/${encodeURIComponent(tableName)}/records`;
    const res=await axiosInstance.get(url,{params:{limit,offset}});
    if(res.status!==200)throw new Error(`Failed fetch rows from ${tableName}: ${res.status}`);
    const data=res.data.list||[];
    collected=collected.concat(data);
    if(data.length<limit)break;
    offset+=limit;
    await sleep(50);
  }
  console.log(`Fetched ${collected.length} rows from ${tableName}`);
  return collected;
}

async function postRowV2(tableName,payload){
  const url=`/api/v2/tables/${encodeURIComponent(tableName)}/records`;
  return (await axiosInstance.post(url,payload)).data;
}

async function patchRowV2(tableName,id,payload){
  const url=`/api/v2/tables/${encodeURIComponent(tableName)}/records/${id}`;
  return (await axiosInstance.patch(url,payload)).data;
}

async function buildTargetCache(){
  const rows=await fetchAllRows(TARGET_TABLE);
  const map={};
  for(const r of rows){
    const keyRaw=safeGet(r,"Company Key")||safeGet(r,"Company Name")||"";
    const key=normalizeCompanyKey(keyRaw);
    const id=getRowId(r);
    map[key]={row:r,id};
  }
  return map;
}

async function upsertCompany(targetCache,payload){
  const key=normalizeCompanyKey(payload["Company Key"]||payload["Company Name"]||"");
  const existing=targetCache[key]||null;
  if(DRY_RUN.toLowerCase()==="true"){
    console.log("[DRY RUN] Payload for",key,":",JSON.stringify(payload,null,2));
    return {dry:true};
  }
  if(existing&&existing.id){
    try{
      const res=await patchRowV2(TARGET_TABLE,existing.id,payload);
      await sleep(Number(RATE_LIMIT_MS));
      return {updated:true,id:existing.id,res};
    }catch(err){
      console.error("Patch failed:",err.message||err);
      throw err;
    }
  }else{
    try{
      const res=await postRowV2(TARGET_TABLE,payload);
      await sleep(Number(RATE_LIMIT_MS));
      return {created:true,res};
    }catch(err){
      console.error("Create failed:",err.message||err);
      throw err;
    }
  }
}

async function main(){
  console.log("Starting GhostScore sync",new Date().toISOString());
  const allRows=await fetchAllRows(SOURCE_TABLE);
  const groups={};
  for(const row of allRows){
    const companyName=(safeGet(row,"Title")||safeGet(row,"Company Name")||"unknown").toString();
    const key=normalizeCompanyKey(companyName);
    if(!groups[key])groups[key]={companyName,rows:[]};
    groups[key].rows.push(row);
  }
  const targetCache=await buildTargetCache();

  for(const [companyKey,group]of Object.entries(groups)){
    const rows=group.rows;
    const N=rows.length;

    const stats={
      sumIncrements:0,
      stageCounts:{},
      assignmentGivenCount:0,
      unpaidCount:0,
      receiptYesCount:0,
      feedbackYesCount:0,
      feedbackNoCount:0,
      followupNoResponseCount:0,
      followupVagueCount:0,
      officialRejectionCount:0,
      aiScreeningCount:0,
      applyAgainYesCount:0,
      recommendYesCount:0,
      roleCounts:{},
      recruiterCounts:{},
      assignmentTypeCounts:{},
      locationCounts:{},
      impactCounts:{},
      firstDate:null,
      lastDate:null,
    };

    for(const r of rows){
      const inc=reportIncrement(r);
      stats.sumIncrements+=inc;
      const stage=safeGet(r,"Where in the Hiring Process Did Ghosting Happen?")||"";
      stats.stageCounts[stage]=(stats.stageCounts[stage]||0)+1;

      const assignment=safeGet(r,"What Type of Assignment Was Given?")||safeGet(r,"Other Assignment Type")||"";
      if(assignment&&!/no assignments required/i.test(assignment))stats.assignmentGivenCount++;

      const paid=(safeGet(r,"Was the interview assignment paid?")||"").toString();
      if(/^\s*no\b/i.test(paid))stats.unpaidCount++;

      const receipt=(safeGet(r,"Did They Confirm Receipt of Your Work?")||"").toString();
      if(/^\s*yes\b/i.test(receipt))stats.receiptYesCount++;

      const feedback=(safeGet(r,"Did You Receive Any Feedback on Your Work?")||"").toString();
      if(/^\s*no\b/i.test(feedback)||/no feedback/i.test(feedback))stats.feedbackNoCount++;
      else if(/^\s*yes\b/i.test(feedback)||/yes/i.test(feedback)){
        if(/vague/i.test(feedback))stats.feedbackYesCount+=0;else stats.feedbackYesCount++;
      }

      const followup=(safeGet(r,"Did You Follow Up After They Stopped Responding?")||"").toString();
      if(/no response/i.test(followup))stats.followupNoResponseCount++;
      else if(/vague/i.test(followup))stats.followupVagueCount++;

      const rej=(safeGet(r,"Did You Receive an Official Rejection?")||"").toString();
      if(/^\s*yes\b/i.test(rej)||/formal email/i.test(rej))stats.officialRejectionCount++;

      const ai=(safeGet(r,"Did the Company Require AI Screening Before Any Interview?")||"").toString();
      if(/^\s*yes\b/i.test(ai))stats.aiScreeningCount++;

      const again=(safeGet(r,"Would You Apply to This Company Again?")||"").toString();
      if(/^\s*yes\b/i.test(again))stats.applyAgainYesCount++;

      const rec=(safeGet(r,"Would You Recommend This Employer?")||"").toString();
      if(/^\s*yes\b/i.test(rec))stats.recommendYesCount++;

      const role=(safeGet(r,"Job Role Applied For")||safeGet(r,"Other Role")||"").toString();
      if(role)stats.roleCounts[role]=(stats.roleCounts[role]||0)+1;

      const recName=(safeGet(r,"Recruiter Name or Email (Optional)")||"").toString();
      if(recName)stats.recruiterCounts[recName]=(stats.recruiterCounts[recName]||0)+1;

      const at=assignment||"";
      if(at)stats.assignmentTypeCounts[at]=(stats.assignmentTypeCounts[at]||0)+1;

      const loc=(safeGet(r,"Company Location")||safeGet(r,"Other Location")||"").toString();
      if(loc)stats.locationCounts[loc]=(stats.locationCounts[loc]||0)+1;

      const impact=(safeGet(r,"How Did This Ghosting Experience Affect You?")||"").toString();
      if(impact){
        const items=impact.toString().split(",").map(x=>x.trim()).filter(Boolean);
        for(const it of items)stats.impactCounts[it]=(stats.impactCounts[it]||0)+1;
      }

      const created=r._created_at||r.createdAt||r.created_at||r.created;
      if(created){
        const d=new Date(created);
        if(!stats.firstDate||d<stats.firstDate)stats.firstDate=d;
        if(!stats.lastDate||d>stats.lastDate)stats.lastDate=d;
      }
    }

    const sum_increments=stats.sumIncrements;
    const damping=N/(N+1);
    const normalized_increment=sum_increments*damping;
    const raw_score=500+normalized_increment;
    const ghostScore=clamp(Math.round(raw_score),0,999);
    const avg_increment=N?Math.round((sum_increments/N)*10)/10:0;

    const noResponseCount=(stats.stageCounts["No Response After Application"]||0)+(stats.stageCounts["No Response After Initial Inquiry"]||0);
    const ghostedAfterAssignmentCount=stats.stageCounts["Ghosted After Completing an Assignment"]||0;
    const ghostedAfterInterviewCount=(stats.stageCounts["Ghosted After First Interview"]||0)+(stats.stageCounts["Ghosted After Multiple Interviews"]||0);
    const ghostedAfterOfferCount=stats.stageCounts["Ghosted After Verbal Offer"]||0;

    const topRoles=topNFromCounts(stats.roleCounts,3);
    const topRecruiters=topNFromCounts(stats.recruiterCounts,3).map(maskRecruiter);
    const topAssignmentTypes=topNFromCounts(stats.assignmentTypeCounts,3);
    const topLocations=topNFromCounts(stats.locationCounts,3);
    const topImpacts=topNFromCounts(stats.impactCounts,3);

    const confidence=calculateConfidence(N);

    const firstDate=stats.firstDate?dayjs(stats.firstDate).format("YYYY-MM-DD"):"";
    const lastDate=stats.lastDate?dayjs(stats.lastDate).format("YYYY-MM-DD"):"";

    const payload={
      "Company Key":companyKey,
      "Company Name":group.companyName,
      "Reports Count":N,
      "Sum Increments":Math.round(sum_increments),
      "Avg Report Increment":avg_increment,
      "GhostScore":ghostScore,
      "No Response Count":noResponseCount,
      "Ghosted After Assignment Count":ghostedAfterAssignmentCount,
      "Ghosted After Interview Count":ghostedAfterInterviewCount,
      "Ghosted After Offer Count":ghostedAfterOfferCount,
      "Assignment Given Count":stats.assignmentGivenCount,
      "Unpaid Assignment Count":stats.unpaidCount,
      "Receipt Confirmed Count":stats.receiptYesCount,
      "Feedback Yes Count":stats.feedbackYesCount,
      "Feedback No Count":stats.feedbackNoCount,
      "Followup No Response Count":stats.followupNoResponseCount,
      "Followup Vague Count":stats.followupVagueCount,
      "Official Rejection Count":stats.officialRejectionCount,
      "AI Screening Count":stats.aiScreeningCount,
      "Apply Again Yes Count":stats.applyAgainYesCount,
      "Recommend Yes Count":stats.recommendYesCount,
      "Top Roles":topRoles.join(", "),
      "Top Recruiters":topRecruiters.join(", "),
      "Top Assignment Types":topAssignmentTypes.join(", "),
      "Top Locations":topLocations.join(", "),
      "Top Impacts":topImpacts.join(", "),
      "Confidence":confidence,
      "First Report Date":firstDate,
      "Last Report Date":lastDate,
      "Last Synced":dayjs().format(),
    };

    try{
      const result=await upsertCompany(targetCache,payload);
      if(result.dry)console.log("[DRY RUN] Would upsert company:",companyKey);
      else if(result.updated)console.log("Updated company:",companyKey);
      else if(result.created)console.log("Created company:",companyKey);
    }catch(err){
      console.error("Error upserting company",companyKey,err.message||err);
    }
  }
  console.log("Finished GhostScore sync",new Date().toISOString());
}

main().catch(e=>{
  console.error("Fatal error:",e);
  process.exit(1);
});
