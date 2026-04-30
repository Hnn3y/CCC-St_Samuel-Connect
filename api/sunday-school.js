import { sundaySchoolSync } from "../lib/sundaySchool.js";

export default async function handler(req, res) {
  const timestamp = new Date().toISOString();
  console.log(`\n${'='.repeat(60)}`);
  console.log(`📚 SUNDAY SCHOOL ENDPOINT CALLED`);
  console.log(`   Time: ${timestamp}`);
  console.log(`${'='.repeat(60)}\n`);

  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const result = await sundaySchoolSync();
    
    res.status(200).json({
      ok: true,
      timestamp,
      ...result
    });
    
  } catch (e) {
    console.error("\n❌ ERROR:", e.message);
    res.status(500).json({
      ok: false,
      timestamp,
      error: e.message,
    });
  }
}
