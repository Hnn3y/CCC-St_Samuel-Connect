import { google } from "googleapis";
import { DateTime } from "luxon";
import nodemailer from "nodemailer";
import sgMail from "@sendgrid/mail";

// ENV VARS: GOOGLE_CLIENT_EMAIL, GOOGLE_PRIVATE_KEY, GOOGLE_PROJECT_ID, EMAIL_PROVIDER, EMAIL_USER, EMAIL_PASS, SENDGRID_API_KEY, GOOGLE_SHEET_ID_SUNDAY
const {
  GOOGLE_CLIENT_EMAIL,
  GOOGLE_PRIVATE_KEY,
  GOOGLE_PROJECT_ID,
  EMAIL_PROVIDER,
  EMAIL_USER,
  EMAIL_PASS,
  SENDGRID_API_KEY,
  GOOGLE_SHEET_ID_SUNDAY, // Different sheet ID for Sunday School
} = process.env;

if (!GOOGLE_SHEET_ID_SUNDAY) throw new Error("Missing GOOGLE_SHEET_ID_SUNDAY env variable");

const SHEET_NAMES = {
  MASTER: "CCC SUNDAY SCHOOL",
  RECIPIENTS: "RECIPIENTS", // List of people to send reminders to
  STATUS_LOG: "Status Log",
};

const REQUIRED_COLUMNS = {
  MASTER: [
    "DATE",
    "1ST LESSON",
    "2ND LESSON",
    "HYMNS",
    "TOPIC", // Optional: Lesson topic
    "TEACHER", // Optional: Who's teaching
  ],

  RECIPIENTS: [
    "Name",
    "Email",
    "Phone Number",
    "Role", // Teacher, Student, Parent, etc.
    "Subscription", // SUBSCRIBED or NOT SUBSCRIBED
  ]
};

export async function sundaySchoolSync() {
  console.log('\n' + '='.repeat(80));
  console.log('📚 SUNDAY SCHOOL REMINDER SYSTEM STARTED');
  console.log('Current Time:', new Date().toISOString());
  console.log('Today:', DateTime.now().toFormat('dd-MM-yyyy'));
  console.log('='.repeat(80) + '\n');
  
  try {
    // 1. Authenticate Google Sheets
    console.log('📝 STEP 1: Authenticating Google Sheets...');
    const sheets = await getSheetsClient();
    console.log('✅ Authentication successful\n');

    // 2. Fetch Sunday School schedule
    console.log('📝 STEP 2: Fetching Sunday School Schedule...');
    let { rows: scheduleRows, header: scheduleHeader } = await fetchSheetRows(sheets, SHEET_NAMES.MASTER);
    console.log(`✅ Fetched ${scheduleRows.length} scheduled lessons\n`);

    // Ensure required columns
    const { header: ensuredScheduleHeader, changed: scheduleChanged } = ensureColumns(scheduleHeader, REQUIRED_COLUMNS.MASTER);
    if (scheduleChanged) {
      console.log('⚠️  Adding missing columns to schedule...');
      await updateSheetHeader(sheets, SHEET_NAMES.MASTER, ensuredScheduleHeader);
      scheduleHeader = ensuredScheduleHeader;
    }

    // 3. Fetch Recipients
    console.log('📝 STEP 3: Fetching Recipients...');
    let { rows: recipientRows, header: recipientHeader } = await fetchSheetRows(sheets, SHEET_NAMES.RECIPIENTS);
    console.log(`✅ Fetched ${recipientRows.length} recipients\n`);

    // Ensure required columns for recipients
    const { header: ensuredRecipientHeader, changed: recipientChanged } = ensureColumns(recipientHeader, REQUIRED_COLUMNS.RECIPIENTS);
    if (recipientChanged) {
      console.log('⚠️  Adding missing columns to recipients...');
      await updateSheetHeader(sheets, SHEET_NAMES.RECIPIENTS, ensuredRecipientHeader);
      recipientHeader = ensuredRecipientHeader;
    }

    // 4. Process schedule
    const processedSchedule = processSchedule(scheduleRows, scheduleHeader);
    console.log(`✅ Processed ${processedSchedule.length} lessons\n`);

    // 5. Process recipients
    const processedRecipients = processRecipients(recipientRows, recipientHeader);
    console.log(`✅ Processed ${processedRecipients.length} recipients\n`);

    // 6. Find upcoming lesson and send reminders
    console.log('📝 STEP 6: Checking for upcoming lessons...');
    const emailResults = await sendSundaySchoolReminders(processedSchedule, processedRecipients);
    console.log(`✅ Email process complete: ${emailResults.sent} sent, ${emailResults.failed} failed\n`);

    // 7. Log to Status Log
    console.log('📝 STEP 7: Writing to Status Log...');
    const logRow = [
      DateTime.now().toISO({ suppressMilliseconds: true }),
      processedSchedule.length,
      processedRecipients.length,
      emailResults.sent,
      emailResults.failed,
      emailResults.failures.join("; ")
    ];
    await appendSheetRow(sheets, SHEET_NAMES.STATUS_LOG, logRow);
    console.log('✅ Status Log updated\n');

    // 8. Return summary
    console.log('='.repeat(80));
    console.log('✅ sundaySchoolSync() COMPLETED SUCCESSFULLY');
    const summary = {
      lessonsScheduled: processedSchedule.length,
      recipients: processedRecipients.length,
      remindersSent: emailResults.sent,
      remindersFailed: emailResults.failed,
      failures: emailResults.failures,
    };
    console.log('FINAL SUMMARY:', JSON.stringify(summary, null, 2));
    console.log('='.repeat(80) + '\n');
    
    return summary;
    
  } catch (error) {
    console.error('\n' + '='.repeat(80));
    console.error('❌ ERROR IN sundaySchoolSync():');
    console.error('Message:', error.message);
    console.error('Stack trace:', error.stack);
    console.error('='.repeat(80) + '\n');
    throw error;
  }
}

// ==== GOOGLE SHEETS HELPERS ====
async function getSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    credentials: {
      client_email: GOOGLE_CLIENT_EMAIL,
      private_key: (GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n"),
      project_id: GOOGLE_PROJECT_ID,
    },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  const sheets = google.sheets({ version: "v4", auth });
  return sheets;
}

async function fetchSheetRows(sheets, sheetName) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID_SUNDAY,
    range: sheetName,
    majorDimension: "ROWS",
  });
  const values = res.data.values || [];
  if (values.length === 0) return { header: [], rows: [] };
  const [header, ...rows] = values;
  return { header, rows };
}

function ensureColumns(header, required) {
  const newHeader = [...header];
  let changed = false;
  required.forEach(col => {
    if (!newHeader.includes(col)) {
      newHeader.push(col);
      changed = true;
    }
  });
  return { header: newHeader, changed };
}

async function updateSheetHeader(sheets, sheetName, header) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: GOOGLE_SHEET_ID_SUNDAY,
    range: `${sheetName}!1:1`,
    valueInputOption: "RAW",
    requestBody: { values: [header] },
  });
}

async function appendSheetRow(sheets, sheetName, row) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: GOOGLE_SHEET_ID_SUNDAY,
    range: sheetName,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: [row] }
  });
}

function processSchedule(rows, header) {
  console.log('\n--- PROCESSING SUNDAY SCHOOL SCHEDULE ---');
  const schedule = rows.map((row, index) => {
    const obj = {};
    header.forEach((col, i) => { obj[col] = (row[i] || "").trim(); });

    // Parse the date
    const dateStr = obj["DATE"];
    const lessonDate = parseDate(dateStr);
    
    if (lessonDate && lessonDate.isValid) {
      obj["PARSED_DATE"] = lessonDate;
      obj["DATE_ISO"] = lessonDate.toISODate();
      console.log(`✅ Lesson ${index + 1}: ${dateStr} -> Topic: ${obj["TOPIC"] || "N/A"}`);
    } else {
      console.log(`⚠️  Lesson ${index + 1}: Invalid date "${dateStr}"`);
      obj["PARSED_DATE"] = null;
      obj["DATE_ISO"] = null;
    }

    return obj;
  });
  console.log('--- END PROCESSING SCHEDULE ---\n');
  return schedule.filter(s => s["PARSED_DATE"]); // Only keep valid dates
}

function processRecipients(rows, header) {
  console.log('\n--- PROCESSING RECIPIENTS ---');
  const recipients = rows.map((row, index) => {
    const obj = {};
    header.forEach((col, i) => { obj[col] = (row[i] || "").trim(); });

    // Ensure required fields
    REQUIRED_COLUMNS.RECIPIENTS.forEach(col => {
      if (!(col in obj)) obj[col] = "";
    });

    const hasEmail = Boolean(obj["Email"]);
    const subscription = (obj["Subscription"] || "SUBSCRIBED").trim().toUpperCase();
    
    console.log(`✅ Recipient ${index + 1}: ${obj["Name"]} (${obj["Role"]}) - ${hasEmail ? obj["Email"] : "No Email"} - ${subscription}`);

    return obj;
  });
  console.log('--- END PROCESSING RECIPIENTS ---\n');
  return recipients;
}

function parseDate(str) {
  if (!str) return null;
  
  const formats = [
    "yyyy-MM-dd",
    "dd/MM/yyyy",
    "MM/dd/yyyy",
    "dd-MM-yyyy",
    "MM-dd-yyyy",
    "d/M/yyyy",
    "d-M-yyyy",
  ];
  
  let dt = DateTime.fromISO(str);
  if (dt.isValid) return dt;
  
  for (const format of formats) {
    dt = DateTime.fromFormat(str, format);
    if (dt.isValid) return dt;
  }
  
  return null;
}

// ==== EMAIL SENDING ====
async function sendSundaySchoolReminders(schedule, recipients) {
  let sent = 0, failed = 0, failures = [];
  const today = DateTime.now().startOf("day");
  const todayStr = today.toISODate();
  
  console.log('\n' + '='.repeat(80));
  console.log('📧 SUNDAY SCHOOL EMAIL PROCESS STARTED');
  console.log(`Today's date: ${todayStr} (${today.toFormat('dd-MM-yyyy')})`);
  console.log(`Total lessons scheduled: ${schedule.length}`);
  console.log(`Total recipients: ${recipients.length}`);
  console.log('='.repeat(80));

  // Find upcoming lessons (next Sunday or within 7 days)
  const upcomingLessons = schedule.filter(lesson => {
    const lessonDate = lesson["PARSED_DATE"];
    const daysUntil = lessonDate.diff(today, "days").days;
    
    // Send reminder if lesson is:
    // - This Sunday (0-7 days away)
    // - Or tomorrow (Saturday reminder for Sunday lesson)
    return daysUntil >= 0 && daysUntil <= 7;
  }).sort((a, b) => a["PARSED_DATE"] - b["PARSED_DATE"]);

  if (upcomingLessons.length === 0) {
    console.log('\n⏭️  No upcoming lessons in the next 7 days - no reminders to send\n');
    return { sent: 0, failed: 0, failures: [], updatedRecipients: recipients };
  }

  // Get the next lesson
  const nextLesson = upcomingLessons[0];
  const lessonDate = nextLesson["PARSED_DATE"];
  const daysUntil = Math.round(lessonDate.diff(today, "days").days);
  
  console.log(`\n📖 NEXT LESSON FOUND:`);
  console.log(`   Date: ${nextLesson["DATE"]} (${daysUntil} days away)`);
  console.log(`   1st Lesson: ${nextLesson["1ST LESSON"]}`);
  console.log(`   2nd Lesson: ${nextLesson["2ND LESSON"]}`);
  console.log(`   Topic: ${nextLesson["TOPIC"] || "N/A"}`);
  console.log(`   Teacher: ${nextLesson["TEACHER"] || "TBA"}`);
  console.log(`   Hymns: ${nextLesson["HYMNS"]}\n`);

  // Decide when to send
  let shouldSend = false;
  let emailType = "";

  if (daysUntil === 0) {
    shouldSend = true;
    emailType = "TODAY";
    console.log('🟡 Lesson is TODAY - sending reminders\n');
  } else if (daysUntil === 1) {
    shouldSend = true;
    emailType = "TOMORROW";
    console.log('🟢 Lesson is TOMORROW - sending advance reminders\n');
  } else if (daysUntil >= 2 && daysUntil <= 7) {
    shouldSend = true;
    emailType = "UPCOMING";
    console.log(`🟢 Lesson in ${daysUntil} days - sending advance reminders\n`);
  }

  if (!shouldSend) {
    console.log('⏭️  Not time to send reminders yet\n');
    return { sent: 0, failed: 0, failures: [], updatedRecipients: recipients };
  }

  // Send to all subscribed recipients
  for (const recipient of recipients) {
    const name = recipient["Name"] || "Member";
    const subscription = (recipient["Subscription"] || "SUBSCRIBED").trim().toUpperCase();
    
    console.log(`\n📋 Checking: ${name}`);
    
    // Check subscription
    if (subscription === "NOT SUBSCRIBED" || subscription === "UNSUBSCRIBED") {
      console.log(`   🚫 NOT SUBSCRIBED - Skipping`);
      continue;
    }

    const to = recipient["Email"];
    if (!to) {
      console.log(`   ⏭️  NO EMAIL - Skipping`);
      continue;
    }

    console.log(`   Email: ${to}`);
    console.log(`   Role: ${recipient["Role"]}`);
    console.log(`   📧 Sending ${emailType} reminder...`);

    try {
      const template = getSundaySchoolEmailTemplate(nextLesson, recipient, daysUntil, emailType);
      await sendEmail(to, template);
      sent++;
      console.log(`   ✅ EMAIL SENT!`);
    } catch (e) {
      failed++;
      const errorMsg = `${name} (${to}): ${e.message}`;
      failures.push(errorMsg);
      console.error(`   ❌ FAILED: ${e.message}`);
    }
  }

  console.log('\n' + '='.repeat(80));
  console.log('📊 EMAIL SUMMARY');
  console.log(`✅ Emails sent: ${sent}`);
  console.log(`❌ Emails failed: ${failed}`);
  if (failures.length > 0) {
    console.log(`\nFailures:\n   - ${failures.join("\n   - ")}`);
  }
  console.log('='.repeat(80) + '\n');

  return { sent, failed, failures, updatedRecipients: recipients };
}

function getSundaySchoolEmailTemplate(lesson, recipient, daysUntil, emailType) {
  const name = recipient["Name"] || "Member";
  const role = recipient["Role"] || "Member";
  
  let subjectPrefix = "";
  let greeting = "";
  
  if (emailType === "TODAY") {
    subjectPrefix = "📚 Today's";
    greeting = `This is a reminder that Sunday School is TODAY!`;
  } else if (emailType === "TOMORROW") {
    subjectPrefix = "📚 Tomorrow's";
    greeting = `This is a reminder that Sunday School is TOMORROW!`;
  }

  return {
    subject: `${subjectPrefix} Sunday School - ${lesson["TOPIC"] || lesson["1ST LESSON"]}`,
    text: (
      `Dear ${name},\n\n` +
      `${greeting}\n\n` +
      `📖 LESSON DETAILS:\n` +
      `Date: ${lesson["DATE"]}\n` +
      `1st Lesson: ${lesson["1ST LESSON"]}\n` +
      `2nd Lesson: ${lesson["2ND LESSON"]}\n` +
      (lesson["TOPIC"] ? `Topic: ${lesson["TOPIC"]}\n` : "") +
      (lesson["TEACHER"] ? `Teacher: ${lesson["TEACHER"]}\n` : "") +
      `Hymns: ${lesson["HYMNS"]}\n\n` +
      (role === "Teacher" ? 
        `As a teacher, please ensure you're prepared for the lesson.\n\n` : 
        `We look forward to seeing you in class!\n\n`) +
      `God bless you,\n` 
     // `CCC Sunday School Team`
    )
  };
}

async function sendEmail(to, { subject, text }) {
  console.log(`      → Provider: ${EMAIL_PROVIDER}`);
  console.log(`      → To: ${to}`);
  console.log(`      → Subject: ${subject}`);
  
  if (EMAIL_PROVIDER === "smtp") {
    const transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST,          
      port: Number(process.env.SMTP_PORT),
      secure: process.env.SMTP_SECURE === "true", 
      auth: {
        user: EMAIL_USER,  
        pass: EMAIL_PASS,  
      },
    });

    await transporter.sendMail({
      from: `"CCC Sunday School" <${EMAIL_USER}>`, 
      to,
      subject,
      text,
    });

  } else if (EMAIL_PROVIDER === "sendgrid") {
    sgMail.setApiKey(SENDGRID_API_KEY);
    await sgMail.send({
      to,
      from: EMAIL_USER,
      subject,
      text,
    });

  } else {
    throw new Error("Unknown EMAIL_PROVIDER: " + EMAIL_PROVIDER);
  }
}