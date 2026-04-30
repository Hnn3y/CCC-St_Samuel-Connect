import { google } from "googleapis";
import { DateTime } from "luxon";

// ENV VARS:
// GOOGLE_CLIENT_EMAIL, GOOGLE_PRIVATE_KEY, GOOGLE_PROJECT_ID, GOOGLE_SHEET_ID_SUNDAY
// WHATSAPP_TOKEN, WHATSAPP_PHONE_ID
// WHATSAPP_PROVIDER = META | CALLMEBOT | SMART
// CALLMEBOT_API_KEY = optional default fallback

const {
  GOOGLE_CLIENT_EMAIL,
  GOOGLE_PRIVATE_KEY,
  GOOGLE_PROJECT_ID,
  GOOGLE_SHEET_ID_SUNDAY,
  WHATSAPP_TOKEN,
  WHATSAPP_PHONE_ID,
  WHATSAPP_PROVIDER = "CALLMEBOT",
  CALLMEBOT_API_KEY
} = process.env;

if (!GOOGLE_SHEET_ID_SUNDAY) throw new Error("Missing GOOGLE_SHEET_ID_SUNDAY");
if (WHATSAPP_PROVIDER === "META") {
  if (!WHATSAPP_TOKEN) throw new Error("Missing WHATSAPP_TOKEN");
  if (!WHATSAPP_PHONE_ID) throw new Error("Missing WHATSAPP_PHONE_ID");
}

const SHEET_NAMES = {
  MASTER: "CCC SUNDAY SCHOOL",
  RECIPIENTS: "CCC RECIPIENTS",
  STATUS_LOG: "Status Log",
};

const REQUIRED_COLUMNS = {
  MASTER: ["DATE","1ST LESSON","2ND LESSON","HYMNS","TOPIC","TEACHER"],
  RECIPIENTS: ["Name","Phone Number","Role","Subscription","CALLMEBOT_API_KEY"]
};

// ================= HELPERS =================

function maskPhone(phone) {
  return phone.slice(0, 3) + "****" + phone.slice(-3);
}

function delay(ms) {
  return new Promise(res => setTimeout(res, ms));
}

function normaliseToChatId(phone) {
  let clean = phone.replace(/[\s\-().+]/g, "");

  if (clean.startsWith("234") && clean.length === 13) return clean;
  if (clean.startsWith("0") && clean.length === 11) return "234" + clean.slice(1);
  if (clean.length === 10 && /^[789]/.test(clean)) return "234" + clean;

  return clean;
}

// ================= WHATSAPP SENDERS =================

// META API
async function sendWhatsApp(phoneNumber, message) {
  const to = normaliseToChatId(phoneNumber);
  console.log(`      → Sending (Meta) to: ${maskPhone(to)}`);

  const res = await fetch(`https://graph.facebook.com/v19.0/${WHATSAPP_PHONE_ID}/messages`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${WHATSAPP_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      messaging_product: "whatsapp",
      to,
      type: "text",
      text: { body: message },
    }),
  });

  const data = await res.json();

  if (!res.ok) {
    throw new Error(data?.error?.message || "Meta API error");
  }
}

// CALLMEBOT
async function sendWhatsAppCallMeBot(phoneNumber, message, apiKeyOverride) {
  const to = normaliseToChatId(phoneNumber);
  const encodedMessage = encodeURIComponent(message);

  const apiKey = apiKeyOverride || CALLMEBOT_API_KEY;
  if (!apiKey) throw new Error("Missing CallMeBot API key");

  console.log(`      → Sending (CallMeBot) to: ${maskPhone(to)}`);

  const url = `https://api.callmebot.com/whatsapp.php?phone=${to}&text=${encodedMessage}&apikey=${apiKey}`;
  const res = await fetch(url);
  const text = await res.text();

  if (!res.ok || !text.toLowerCase().includes("message")) {
    throw new Error(text);
  }
}

// SMART ROUTER
async function sendWhatsAppUnified(phoneNumber, message, recipient) {
  const provider = WHATSAPP_PROVIDER.toUpperCase();

  if (provider === "META") {
    return sendWhatsApp(phoneNumber, message);
  }

  if (provider === "CALLMEBOT") {
    return sendWhatsAppCallMeBot(
      phoneNumber,
      message,
      recipient["CALLMEBOT_API_KEY"]
    );
  }

  if (provider === "SMART") {
    try {
      return await sendWhatsApp(phoneNumber, message);
    } catch (e) {
      console.warn("⚠️ Meta failed → fallback to CallMeBot");

      const apiKey = recipient["CALLMEBOT_API_KEY"];
      if (!apiKey) throw new Error("No CallMeBot key");

      return sendWhatsAppCallMeBot(phoneNumber, message, apiKey);
    }
  }

  throw new Error("Invalid WHATSAPP_PROVIDER");
}

// ================= MAIN =================

export async function sundaySchoolSync() {
  const sheets = await getSheetsClient();

  let { rows: scheduleRows, header: scheduleHeader } =
    await fetchSheetRows(sheets, SHEET_NAMES.MASTER);

  let { rows: recipientRows, header: recipientHeader } =
    await fetchSheetRows(sheets, SHEET_NAMES.RECIPIENTS);

  scheduleHeader = ensureColumns(scheduleHeader, REQUIRED_COLUMNS.MASTER).header;
  recipientHeader = ensureColumns(recipientHeader, REQUIRED_COLUMNS.RECIPIENTS).header;

  const schedule = processSchedule(scheduleRows, scheduleHeader);
  const recipients = processRecipients(recipientRows, recipientHeader);

  return sendSundaySchoolReminders(schedule, recipients);
}

// ================= CORE LOGIC =================

async function sendSundaySchoolReminders(schedule, recipients) {
  let sent = 0, failed = 0;

  const today = DateTime.now().startOf("day");

  const upcoming = schedule
    .filter(l => l.PARSED_DATE.diff(today, "days").days >= 0)
    .sort((a,b)=>a.PARSED_DATE-b.PARSED_DATE);

  if (!upcoming.length) return { sent, failed };

  const lesson = upcoming[0];

  for (const recipient of recipients) {
    const phone = recipient["Phone Number"];
    const sub = (recipient["Subscription"] || "").toUpperCase();

    if (!phone || sub === "UNSUBSCRIBED") continue;

    // AUTO SKIP if CallMeBot required but no key
    if (WHATSAPP_PROVIDER !== "META" && !recipient["CALLMEBOT_API_KEY"]) {
      console.log("⏭️ Skipping (no CallMeBot key)");
      continue;
    }

    try {
      const msg = getSundaySchoolWhatsAppMessage(lesson, recipient);

      await sendWhatsAppUnified(phone, msg, recipient);

      sent++;
      await delay(1000); // rate limit
    } catch (e) {
      failed++;
      console.error(`❌ Failed: ${e.message}`);
    }
  }

  return { sent, failed };
}

// ================= PROCESSING =================

function processSchedule(rows, header) {
  return rows.map(row => {
    const obj = {};
    header.forEach((h,i)=>obj[h]=(row[i]||"").trim());

    const dt = DateTime.fromISO(obj.DATE);
    obj.PARSED_DATE = dt.isValid ? dt : null;

    return obj;
  }).filter(x=>x.PARSED_DATE);
}

function processRecipients(rows, header) {
  return rows.map(row => {
    const obj = {};
    header.forEach((h,i)=>obj[h]=(row[i]||"").trim());
    return obj;
  });
}

// ================= GOOGLE =================

async function getSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    credentials: {
      client_email: GOOGLE_CLIENT_EMAIL,
      private_key: GOOGLE_PRIVATE_KEY.replace(/\\n/g,"\n"),
      project_id: GOOGLE_PROJECT_ID,
    },
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  return google.sheets({ version: "v4", auth });
}

async function fetchSheetRows(sheets, name) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID_SUNDAY,
    range: name,
  });

  const values = res.data.values || [];
  const [header,...rows] = values;

  return { header: header||[], rows };
}

function ensureColumns(header, required) {
  const newHeader = [...header];
  required.forEach(col => {
    if (!newHeader.includes(col)) newHeader.push(col);
  });
  return { header: newHeader };
}

// ================= MESSAGE =================

function getSundaySchoolWhatsAppMessage(lesson, recipient) {
  return `✝️ CCC Sunday School Reminder

Dear ${recipient["Name"]},

📅 Date: ${lesson.DATE}
📖 1st Lesson: ${lesson["1ST LESSON"]}
📖 2nd Lesson: ${lesson["2ND LESSON"]}

God bless you 🙏`;
}