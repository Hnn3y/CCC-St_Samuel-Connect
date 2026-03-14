import { google } from "googleapis";
import { DateTime } from "luxon";
import nodemailer from "nodemailer";
import sgMail from "@sendgrid/mail";
import twilio from "twilio"; // npm install twilio

// ENV VARS: GOOGLE_CLIENT_EMAIL, GOOGLE_PRIVATE_KEY, GOOGLE_PROJECT_ID, EMAIL_PROVIDER, EMAIL_USER, EMAIL_PASS, SENDGRID_API_KEY, GOOGLE_SHEET_ID_SUNDAY
// WHATSAPP ENV VARS: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM
//   TWILIO_WHATSAPP_FROM format: "whatsapp:+1XXXXXXXXXX" (your Twilio WhatsApp sender number)
const {
  GOOGLE_CLIENT_EMAIL,
  GOOGLE_PRIVATE_KEY,
  GOOGLE_PROJECT_ID,
  EMAIL_PROVIDER,
  EMAIL_USER,
  EMAIL_PASS,
  SENDGRID_API_KEY,
  GOOGLE_SHEET_ID_SUNDAY,
  TWILIO_ACCOUNT_SID,
  TWILIO_AUTH_TOKEN,
  TWILIO_WHATSAPP_FROM, // e.g. "whatsapp:+14155238886"
} = process.env;

if (!GOOGLE_SHEET_ID_SUNDAY) throw new Error("Missing GOOGLE_SHEET_ID_SUNDAY env variable");

const SHEET_NAMES = {
  MASTER: "CCC SUNDAY SCHOOL",
  RECIPIENTS: "RECIPIENTS",
  STATUS_LOG: "Status Log",
};

const REQUIRED_COLUMNS = {
  MASTER: [
    "DATE",
    "1ST LESSON",
    "2ND LESSON",
    "HYMNS",
    "TOPIC",
    "TEACHER",
  ],
  RECIPIENTS: [
    "Name",
    "Email",
    "Phone Number",
    "Role",
    "Subscription",
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

    // 6. Find upcoming lesson and send reminders (email + WhatsApp)
    console.log('📝 STEP 6: Checking for upcoming lessons...');
    const results = await sendSundaySchoolReminders(processedSchedule, processedRecipients);
    console.log(`✅ Reminders complete:`);
    console.log(`   Email  → sent: ${results.emailSent}, failed: ${results.emailFailed}`);
    console.log(`   WhatsApp → sent: ${results.whatsappSent}, failed: ${results.whatsappFailed}\n`);

    // 7. Log to Status Log
    console.log('📝 STEP 7: Writing to Status Log...');
    const logRow = [
      DateTime.now().toISO({ suppressMilliseconds: true }),
      processedSchedule.length,
      processedRecipients.length,
      results.emailSent,
      results.emailFailed,
      results.whatsappSent,
      results.whatsappFailed,
      [...results.emailFailures, ...results.whatsappFailures].join("; ")
    ];
    await appendSheetRow(sheets, SHEET_NAMES.STATUS_LOG, logRow);
    console.log('✅ Status Log updated\n');

    // 8. Return summary
    console.log('='.repeat(80));
    console.log('✅ sundaySchoolSync() COMPLETED SUCCESSFULLY');
    const summary = {
      lessonsScheduled: processedSchedule.length,
      recipients: processedRecipients.length,
      emailSent: results.emailSent,
      emailFailed: results.emailFailed,
      whatsappSent: results.whatsappSent,
      whatsappFailed: results.whatsappFailed,
      failures: [...results.emailFailures, ...results.whatsappFailures],
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
  return schedule.filter(s => s["PARSED_DATE"]);
}

function processRecipients(rows, header) {
  console.log('\n--- PROCESSING RECIPIENTS ---');
  const recipients = rows.map((row, index) => {
    const obj = {};
    header.forEach((col, i) => { obj[col] = (row[i] || "").trim(); });

    REQUIRED_COLUMNS.RECIPIENTS.forEach(col => {
      if (!(col in obj)) obj[col] = "";
    });

    const hasEmail = Boolean(obj["Email"]);
    const hasPhone = Boolean(obj["Phone Number"]);
    const subscription = (obj["Subscription"] || "SUBSCRIBED").trim().toUpperCase();
    
    console.log(`✅ Recipient ${index + 1}: ${obj["Name"]} (${obj["Role"]}) - Email: ${hasEmail ? obj["Email"] : "None"} - Phone: ${hasPhone ? obj["Phone Number"] : "None"} - ${subscription}`);

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

// ==== REMINDER ORCHESTRATION ====
async function sendSundaySchoolReminders(schedule, recipients) {
  let emailSent = 0, emailFailed = 0, emailFailures = [];
  let whatsappSent = 0, whatsappFailed = 0, whatsappFailures = [];

  const today = DateTime.now().startOf("day");
  const todayStr = today.toISODate();
  
  console.log('\n' + '='.repeat(80));
  console.log('📨 SUNDAY SCHOOL REMINDER PROCESS STARTED');
  console.log(`Today's date: ${todayStr}`);
  console.log(`Total lessons: ${schedule.length} | Total recipients: ${recipients.length}`);
  console.log('='.repeat(80));

  // Find the next upcoming lesson within 7 days
  const upcomingLessons = schedule.filter(lesson => {
    const daysUntil = lesson["PARSED_DATE"].diff(today, "days").days;
    return daysUntil >= 0 && daysUntil <= 7;
  }).sort((a, b) => a["PARSED_DATE"] - b["PARSED_DATE"]);

  if (upcomingLessons.length === 0) {
    console.log('\n⏭️  No upcoming lessons in the next 7 days\n');
    return { emailSent, emailFailed, emailFailures, whatsappSent, whatsappFailed, whatsappFailures };
  }

  const nextLesson = upcomingLessons[0];
  const lessonDate = nextLesson["PARSED_DATE"];
  const daysUntil = Math.round(lessonDate.diff(today, "days").days);

  console.log(`\n📖 NEXT LESSON:`);
  console.log(`   Date: ${nextLesson["DATE"]} (${daysUntil} day(s) away)`);
  console.log(`   1st Lesson: ${nextLesson["1ST LESSON"]}`);
  console.log(`   2nd Lesson: ${nextLesson["2ND LESSON"]}`);
  console.log(`   Topic: ${nextLesson["TOPIC"] || "N/A"}`);
  console.log(`   Teacher: ${nextLesson["TEACHER"] || "TBA"}`);
  console.log(`   Hymns: ${nextLesson["HYMNS"]}\n`);

  let emailType = "";
  if (daysUntil === 0) emailType = "TODAY";
  else if (daysUntil === 1) emailType = "TOMORROW";
  else if (daysUntil >= 2 && daysUntil <= 7) emailType = "UPCOMING";
  else {
    console.log('⏭️  Not time to send reminders yet\n');
    return { emailSent, emailFailed, emailFailures, whatsappSent, whatsappFailed, whatsappFailures };
  }

  // Send to all subscribed recipients
  for (const recipient of recipients) {
    const name = recipient["Name"] || "Member";
    const subscription = (recipient["Subscription"] || "SUBSCRIBED").trim().toUpperCase();

    console.log(`\n📋 Processing: ${name}`);

    if (subscription === "NOT SUBSCRIBED" || subscription === "UNSUBSCRIBED") {
      console.log(`   🚫 NOT SUBSCRIBED - Skipping`);
      continue;
    }

    // --- EMAIL ---
    const emailAddress = recipient["Email"];
    if (emailAddress) {
      console.log(`   📧 Sending email to ${emailAddress}...`);
      try {
        const emailTemplate = getSundaySchoolEmailTemplate(nextLesson, recipient, daysUntil, emailType);
        await sendEmail(emailAddress, emailTemplate);
        emailSent++;
        console.log(`   ✅ Email sent!`);
      } catch (e) {
        emailFailed++;
        emailFailures.push(`EMAIL | ${name} (${emailAddress}): ${e.message}`);
        console.error(`   ❌ Email failed: ${e.message}`);
      }
    } else {
      console.log(`   ⏭️  No email address - skipping email`);
    }

    // --- WHATSAPP ---
    const phoneNumber = recipient["Phone Number"];
    if (phoneNumber) {
      console.log(`   💬 Sending WhatsApp to ${phoneNumber}...`);
      try {
        const whatsappMessage = getSundaySchoolWhatsAppMessage(nextLesson, recipient, daysUntil, emailType);
        await sendWhatsApp(phoneNumber, whatsappMessage);
        whatsappSent++;
        console.log(`   ✅ WhatsApp sent!`);
      } catch (e) {
        whatsappFailed++;
        whatsappFailures.push(`WHATSAPP | ${name} (${phoneNumber}): ${e.message}`);
        console.error(`   ❌ WhatsApp failed: ${e.message}`);
      }
    } else {
      console.log(`   ⏭️  No phone number - skipping WhatsApp`);
    }
  }

  console.log('\n' + '='.repeat(80));
  console.log('📊 REMINDER SUMMARY');
  console.log(`📧 Email    → Sent: ${emailSent} | Failed: ${emailFailed}`);
  console.log(`💬 WhatsApp → Sent: ${whatsappSent} | Failed: ${whatsappFailed}`);
  if (emailFailures.length > 0 || whatsappFailures.length > 0) {
    console.log(`\nFailures:\n   - ${[...emailFailures, ...whatsappFailures].join("\n   - ")}`);
  }
  console.log('='.repeat(80) + '\n');

  return { emailSent, emailFailed, emailFailures, whatsappSent, whatsappFailed, whatsappFailures };
}

// ==== MESSAGE TEMPLATES ====
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
  } else {
    subjectPrefix = "📚 Upcoming";
    greeting = `This is a reminder that Sunday School is in ${daysUntil} days!`;
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
      (role === "Teacher"
        ? `As a teacher, please ensure you're prepared for the lesson.\n\n`
        : `We look forward to seeing you in class!\n\n`) +
      `God bless you,\n`
    )
  };
}

/**
 * Builds a concise WhatsApp message for a recipient.
 * WhatsApp messages should be shorter and more conversational than emails.
 */
function getSundaySchoolWhatsAppMessage(lesson, recipient, daysUntil, emailType) {
  const name = recipient["Name"] || "Member";
  const role = recipient["Role"] || "Member";

  let timeLabel = "";
  if (emailType === "TODAY") timeLabel = "*TODAY* 🙏";
  else if (emailType === "TOMORROW") timeLabel = "*TOMORROW* 🌅";
  else timeLabel = `*in ${daysUntil} days* 📅`;

  const lines = [
    `✝️ *CCC Sunday School Reminder*`,
    ``,
    `Dear ${name}, Sunday School is ${timeLabel}!`,
    ``,
    `📖 *Lesson Details*`,
    `📅 Date: ${lesson["DATE"]}`,
    `1️⃣  1st Lesson: ${lesson["1ST LESSON"]}`,
    `2️⃣  2nd Lesson: ${lesson["2ND LESSON"]}`,
  ];

  if (lesson["TOPIC"]) lines.push(`📝 Topic: ${lesson["TOPIC"]}`);
  if (lesson["TEACHER"]) lines.push(`👨‍🏫 Teacher: ${lesson["TEACHER"]}`);
  if (lesson["HYMNS"]) lines.push(`🎵 Hymns: ${lesson["HYMNS"]}`);

  lines.push(``);
  lines.push(role === "Teacher"
    ? `As a teacher, please come prepared. 🙌`
    : `We look forward to seeing you! 😊`
  );
  lines.push(``);
  lines.push(`God bless you 🙏`);

  return lines.join("\n");
}

// ==== WHATSAPP SENDING (Twilio) ====
/**
 * Sends a WhatsApp message via Twilio.
 *
 * SETUP REQUIREMENTS:
 *   1. npm install twilio
 *   2. Set env vars: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM
 *   3. TWILIO_WHATSAPP_FROM = "whatsapp:+<your_twilio_number>"
 *      e.g. "whatsapp:+14155238886" for the Twilio sandbox
 *
 * PHONE NUMBER FORMAT in your Google Sheet's "Phone Number" column:
 *   - Must be in E.164 format WITH country code, e.g. +2348012345678 (Nigeria)
 *   - No spaces, dashes, or brackets
 *
 * PRODUCTION vs SANDBOX:
 *   - Sandbox (free testing): recipients must first send "join <sandbox-word>"
 *     to your Twilio sandbox number.
 *   - Production: requires a WhatsApp Business Account approved by Meta.
 *
 * ALTERNATIVE PROVIDERS (if you don't want Twilio):
 *   - Vonage (formerly Nexmo): similar API, npm install @vonage/server-sdk
 *   - 360dialog: popular for West Africa, direct WhatsApp Business API access
 *   - WatiAPI / Wati.io: no-code friendly, WhatsApp-focused
 *   - WhatsApp Cloud API (Meta): free but requires more setup via Meta for Developers
 */
async function sendWhatsApp(phoneNumber, message) {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN || !TWILIO_WHATSAPP_FROM) {
    throw new Error(
      "Missing WhatsApp config. Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, and TWILIO_WHATSAPP_FROM."
    );
  }

  // Normalise phone number: ensure it starts with "whatsapp:+"
  const normalised = normaliseWhatsAppNumber(phoneNumber);

  const client = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);

  const msg = await client.messages.create({
    from: TWILIO_WHATSAPP_FROM,
    to: normalised,
    body: message,
  });

  console.log(`      → Twilio SID: ${msg.sid} | Status: ${msg.status}`);
}

/**
 * Ensures the phone number is in WhatsApp/Twilio format: "whatsapp:+XXXXXXXXXXX"
 * Handles numbers like:
 *   - +2348012345678  → whatsapp:+2348012345678
 *   - 2348012345678   → whatsapp:+2348012345678
 *   - 08012345678     → whatsapp:+2348012345678  (Nigerian local, auto-prefixed)
 *   - whatsapp:+234...  → unchanged
 */
function normaliseWhatsAppNumber(phone) {
  let clean = phone.replace(/[\s\-().]/g, "");

  // Already fully formatted
  if (clean.startsWith("whatsapp:+")) return clean;
  clean = clean.replace(/^whatsapp:/i, "");

  if (!clean.startsWith("+")) {
    if (clean.startsWith("234") && clean.length === 13) {
      // 2348012345678 — has country code but missing "+"
      clean = "+" + clean;
    } else if (clean.startsWith("0") && clean.length === 11) {
      // 08012345678 — local format with leading 0
      clean = "+234" + clean.slice(1);
    } else if (clean.length === 10 && /^[789]/.test(clean)) {
      // 8012345678 — local format without leading 0 (MTN, Airtel, Glo, 9mobile)
      clean = "+234" + clean;
    } else {
      // Fallback: just prepend +
      clean = "+" + clean;
    }
  }

  return "whatsapp:" + clean;
}

// ==== EMAIL SENDING ====
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
