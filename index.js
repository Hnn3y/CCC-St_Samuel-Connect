import { google } from "googleapis";
import { DateTime } from "luxon";
import pkg from "whatsapp-web.js";
const { Client, LocalAuth } = pkg;
import qrcode from "qrcode-terminal";

// ENV VARS: GOOGLE_CLIENT_EMAIL, GOOGLE_PRIVATE_KEY, GOOGLE_PROJECT_ID, GOOGLE_SHEET_ID_SUNDAY
const {
  GOOGLE_CLIENT_EMAIL,
  GOOGLE_PRIVATE_KEY,
  GOOGLE_PROJECT_ID,
  GOOGLE_SHEET_ID_SUNDAY,
} = process.env;

if (!GOOGLE_SHEET_ID_SUNDAY) throw new Error("Missing GOOGLE_SHEET_ID_SUNDAY env variable");

const SHEET_NAMES = {
  MASTER: "CCC SUNDAY SCHOOL",
  RECIPIENTS: "CCC RECIPIENTS",
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
    "Phone Number",
    "Role",
    "Subscription",
  ]
};

// ==== WHATSAPP CLIENT ====
let clientInstance = null;
let clientReady = false;

function initWhatsApp() {
  return new Promise((resolve, reject) => {
    if (clientReady && clientInstance) {
      console.log("✅ WhatsApp client already initialised");
      return resolve(clientInstance);
    }

    console.log("🔄 Initialising WhatsApp Web client...");

    const client = new Client({
      authStrategy: new LocalAuth({ dataPath: ".wwebjs_auth" }),
      puppeteer: {
        headless: true,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-dev-shm-usage",
        ],
      },
    });

    client.on("qr", (qr) => {
      console.log("\n" + "=".repeat(60));
      console.log("📱 WHATSAPP QR CODE — Scan with your phone:");
      console.log("   WhatsApp → Linked Devices → Link a Device");
      console.log("=".repeat(60));
      qrcode.generate(qr, { small: true });
      console.log("=".repeat(60) + "\n");
    });

    client.on("ready", () => {
      console.log("✅ WhatsApp Web client is READY");
      clientReady = true;
      resolve(client);
    });

    client.on("auth_failure", (msg) => {
      console.error("❌ WhatsApp authentication failed:", msg);
      clientReady = false;
      reject(new Error("WhatsApp auth failed: " + msg));
    });

    client.on("disconnected", (reason) => {
      console.warn("⚠️  WhatsApp client disconnected:", reason);
      clientReady = false;
      clientInstance = null;
    });

    clientInstance = client;
    client.initialize();
  });
}

async function sendWhatsApp(phoneNumber, message) {
  if (!clientReady || !clientInstance) {
    throw new Error("WhatsApp client is not ready. Call initWhatsApp() first.");
  }

  const chatId = normaliseToChatId(phoneNumber);
  console.log(`      → WhatsApp Chat ID: ${chatId}`);

  const isRegistered = await clientInstance.isRegisteredUser(chatId);
  if (!isRegistered) {
    throw new Error(`Number ${phoneNumber} is not registered on WhatsApp`);
  }

  await clientInstance.sendMessage(chatId, message);
}

function normaliseToChatId(phone) {
  let clean = phone.replace(/[\s\-().+]/g, "");

  if (clean.startsWith("234") && clean.length === 13) {
    return clean + "@c.us";
  } else if (clean.startsWith("0") && clean.length === 11) {
    return "234" + clean.slice(1) + "@c.us";
  } else if (clean.length === 10 && /^[789]/.test(clean)) {
    return "234" + clean + "@c.us";
  } else {
    return clean + "@c.us";
  }
}

// ==== MAIN EXPORT ====
export async function sundaySchoolSync() {
  console.log('\n' + '='.repeat(80));
  console.log('📚 SUNDAY SCHOOL REMINDER SYSTEM STARTED');
  console.log('Current Time:', new Date().toISOString());
  console.log('Today:', DateTime.now().toFormat('dd-MM-yyyy'));
  console.log('='.repeat(80) + '\n');
  
  try {
    // 1. Initialise WhatsApp client
    console.log('📝 STEP 1: Initialising WhatsApp client...');
    await initWhatsApp();
    console.log('✅ WhatsApp client ready\n');

    // 2. Authenticate Google Sheets
    console.log('📝 STEP 2: Authenticating Google Sheets...');
    const sheets = await getSheetsClient();
    console.log('✅ Authentication successful\n');

    // 3. Fetch Sunday School schedule
    console.log('📝 STEP 3: Fetching Sunday School Schedule...');
    let { rows: scheduleRows, header: scheduleHeader } = await fetchSheetRows(sheets, SHEET_NAMES.MASTER);
    console.log(`✅ Fetched ${scheduleRows.length} scheduled lessons\n`);

    const { header: ensuredScheduleHeader, changed: scheduleChanged } = ensureColumns(scheduleHeader, REQUIRED_COLUMNS.MASTER);
    if (scheduleChanged) {
      console.log('⚠️  Adding missing columns to schedule...');
      await updateSheetHeader(sheets, SHEET_NAMES.MASTER, ensuredScheduleHeader);
      scheduleHeader = ensuredScheduleHeader;
    }

    // 4. Fetch Recipients
    console.log('📝 STEP 4: Fetching Recipients...');
    let { rows: recipientRows, header: recipientHeader } = await fetchSheetRows(sheets, SHEET_NAMES.RECIPIENTS);
    console.log(`✅ Fetched ${recipientRows.length} recipients\n`);

    const { header: ensuredRecipientHeader, changed: recipientChanged } = ensureColumns(recipientHeader, REQUIRED_COLUMNS.RECIPIENTS);
    if (recipientChanged) {
      console.log('⚠️  Adding missing columns to recipients...');
      await updateSheetHeader(sheets, SHEET_NAMES.RECIPIENTS, ensuredRecipientHeader);
      recipientHeader = ensuredRecipientHeader;
    }

    // 5. Process schedule and recipients
    const processedSchedule = processSchedule(scheduleRows, scheduleHeader);
    console.log(`✅ Processed ${processedSchedule.length} lessons\n`);

    const processedRecipients = processRecipients(recipientRows, recipientHeader);
    console.log(`✅ Processed ${processedRecipients.length} recipients\n`);

    // 6. Find upcoming lesson and send WhatsApp reminders
    console.log('📝 STEP 6: Checking for upcoming lessons...');
    const results = await sendSundaySchoolReminders(processedSchedule, processedRecipients);
    console.log(`✅ Reminders complete: sent: ${results.whatsappSent}, failed: ${results.whatsappFailed}\n`);

    // 7. Log to Status Log
    console.log('📝 STEP 7: Writing to Status Log...');
    const logRow = [
      DateTime.now().toISO({ suppressMilliseconds: true }),
      processedSchedule.length,
      processedRecipients.length,
      results.whatsappSent,
      results.whatsappFailed,
      results.whatsappFailures.join("; ")
    ];
    await appendSheetRow(sheets, SHEET_NAMES.STATUS_LOG, logRow);
    console.log('✅ Status Log updated\n');

    // 8. Return summary
    console.log('='.repeat(80));
    console.log('✅ sundaySchoolSync() COMPLETED SUCCESSFULLY');
    const summary = {
      lessonsScheduled: processedSchedule.length,
      recipients: processedRecipients.length,
      whatsappSent: results.whatsappSent,
      whatsappFailed: results.whatsappFailed,
      failures: results.whatsappFailures,
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
  return google.sheets({ version: "v4", auth });
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

// ==== DATA PROCESSING ====
function processSchedule(rows, header) {
  console.log('\n--- PROCESSING SUNDAY SCHOOL SCHEDULE ---');
  const schedule = rows.map((row, index) => {
    const obj = {};
    header.forEach((col, i) => { obj[col] = (row[i] || "").trim(); });

    const lessonDate = parseDate(obj["DATE"]);
    if (lessonDate && lessonDate.isValid) {
      obj["PARSED_DATE"] = lessonDate;
      obj["DATE_ISO"] = lessonDate.toISODate();
      console.log(`✅ Lesson ${index + 1}: ${obj["DATE"]} -> Topic: ${obj["TOPIC"] || "N/A"}`);
    } else {
      console.log(`⚠️  Lesson ${index + 1}: Invalid date "${obj["DATE"]}"`);
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

    const subscription = (obj["Subscription"] || "SUBSCRIBED").trim().toUpperCase();
    console.log(`✅ Recipient ${index + 1}: ${obj["Name"]} (${obj["Role"]}) - Phone: ${obj["Phone Number"] || "None"} - ${subscription}`);

    return obj;
  });
  console.log('--- END PROCESSING RECIPIENTS ---\n');
  return recipients;
}

function parseDate(str) {
  if (!str) return null;
  const formats = [
    "yyyy-MM-dd", "dd/MM/yyyy", "MM/dd/yyyy",
    "dd-MM-yyyy", "MM-dd-yyyy", "d/M/yyyy", "d-M-yyyy",
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
  let whatsappSent = 0, whatsappFailed = 0, whatsappFailures = [];

  const today = DateTime.now().startOf("day");

  console.log('\n' + '='.repeat(80));
  console.log('💬 SUNDAY SCHOOL WHATSAPP REMINDER PROCESS STARTED');
  console.log(`Today: ${today.toISODate()} | Lessons: ${schedule.length} | Recipients: ${recipients.length}`);
  console.log('='.repeat(80));

  const upcomingLessons = schedule.filter(lesson => {
    const daysUntil = lesson["PARSED_DATE"].diff(today, "days").days;
    return daysUntil >= 0 && daysUntil <= 7;
  }).sort((a, b) => a["PARSED_DATE"] - b["PARSED_DATE"]);

  if (upcomingLessons.length === 0) {
    console.log('\n⏭️  No upcoming lessons in the next 7 days\n');
    return { whatsappSent, whatsappFailed, whatsappFailures };
  }

  const nextLesson = upcomingLessons[0];
  const daysUntil = Math.round(nextLesson["PARSED_DATE"].diff(today, "days").days);

  console.log(`\n📖 NEXT LESSON:`);
  console.log(`   Date: ${nextLesson["DATE"]} (${daysUntil} day(s) away)`);
  console.log(`   1st Lesson: ${nextLesson["1ST LESSON"]}`);
  console.log(`   2nd Lesson: ${nextLesson["2ND LESSON"]}`);
  console.log(`   Topic: ${nextLesson["TOPIC"] || "N/A"}`);
  console.log(`   Teacher: ${nextLesson["TEACHER"] || "TBA"}`);
  console.log(`   Hymns: ${nextLesson["HYMNS"]}\n`);

  let reminderType = "";
  if (daysUntil === 0)      reminderType = "TODAY";
  else if (daysUntil === 1) reminderType = "TOMORROW";
  else if (daysUntil <= 7)  reminderType = "UPCOMING";
  else {
    console.log('⏭️  Not time to send reminders yet\n');
    return { whatsappSent, whatsappFailed, whatsappFailures };
  }

  for (const recipient of recipients) {
    const name = recipient["Name"] || "Member";
    const subscription = (recipient["Subscription"] || "SUBSCRIBED").trim().toUpperCase();

    console.log(`\n📋 Processing: ${name}`);

    if (subscription === "NOT SUBSCRIBED" || subscription === "UNSUBSCRIBED") {
      console.log(`   🚫 NOT SUBSCRIBED - Skipping`);
      continue;
    }

    const phoneNumber = recipient["Phone Number"];
    if (!phoneNumber) {
      console.log(`   ⏭️  No phone number - Skipping`);
      continue;
    }

    console.log(`   💬 Sending WhatsApp to ${phoneNumber}...`);
    try {
      const message = getSundaySchoolWhatsAppMessage(nextLesson, recipient, daysUntil, reminderType);
      await sendWhatsApp(phoneNumber, message);
      whatsappSent++;
      console.log(`   ✅ WhatsApp sent!`);
    } catch (e) {
      whatsappFailed++;
      whatsappFailures.push(`${name} (${phoneNumber}): ${e.message}`);
      console.error(`   ❌ WhatsApp failed: ${e.message}`);
    }
  }

  console.log('\n' + '='.repeat(80));
  console.log('📊 REMINDER SUMMARY');
  console.log(`💬 WhatsApp → Sent: ${whatsappSent} | Failed: ${whatsappFailed}`);
  if (whatsappFailures.length > 0) {
    console.log(`\nFailures:\n   - ${whatsappFailures.join("\n   - ")}`);
  }
  console.log('='.repeat(80) + '\n');

  return { whatsappSent, whatsappFailed, whatsappFailures };
}

// ==== WHATSAPP MESSAGE TEMPLATE ====
function getSundaySchoolWhatsAppMessage(lesson, recipient, daysUntil, reminderType) {
  const name = recipient["Name"] || "Member";
  const role = recipient["Role"] || "Member";

  let timeLabel = "";
  if (reminderType === "TODAY")         timeLabel = "*TODAY* 🙏";
  else if (reminderType === "TOMORROW") timeLabel = "*TOMORROW* 🌅";
  else                                  timeLabel = `*in ${daysUntil} days* 📅`;

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

  if (lesson["TOPIC"])   lines.push(`📝 Topic: ${lesson["TOPIC"]}`);
  if (lesson["TEACHER"]) lines.push(`👨‍🏫 Teacher: ${lesson["TEACHER"]}`);
  if (lesson["HYMNS"])   lines.push(`🎵 Hymns: ${lesson["HYMNS"]}`);

  lines.push(``);
  lines.push(role === "Teacher"
    ? `As a teacher, please come prepared. 🙌`
    : `We look forward to seeing you! 😊`
  );
  lines.push(``);
  lines.push(`God bless you 🙏`);

  return lines.join("\n");
}