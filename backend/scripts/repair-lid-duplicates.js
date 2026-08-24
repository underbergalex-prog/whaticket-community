#!/usr/bin/env node
/* eslint-disable no-console, no-await-in-loop */
/**
 * Einmalige Daten-Reparatur: LID-Doubletten zusammenfuehren.
 *
 * Hintergrund: WhatsApp stellt auf LID-Adressierung um (@lid statt
 * @s.whatsapp.net). Vor dem Fix in convertToContactPayload/
 * CreateOrUpdateContactService legte whaticket fuer LID-Nachrichten ohne
 * mitgelieferte echte Nummer einen ZWEITEN Kontakt mit Pseudo-Nummer an
 * (15-16 Ziffern, z. B. "161340597477621") - derselbe Mensch hatte dann
 * zwei Chats.
 *
 * Was das Skript tut (pro gefundenem Paar, in einer Transaktion):
 *   1. Tickets  des Pseudo-Kontakts  -> echter Kontakt
 *   2. Messages des Pseudo-Kontakts  -> echter Kontakt
 *      (Messages.contactId hat ON DELETE CASCADE - ohne Umhaengen
 *      wuerde der Chat-Verlauf geloescht!)
 *   3. Pseudo-Kontakt loeschen
 *   4. LID auf dem echten Kontakt nachtragen (falls dort noch leer)
 *
 * Matching-Regeln:
 *   A) DETERMINISTISCH (Standard): echter Kontakt traegt lid = "<x>@lid"
 *      und ein anderer Kontakt hat number = "<x>"  -> eindeutig dieselbe
 *      Person, wird gemerged.
 *   B) NAME-HEURISTIK (nur mit --names): Pseudo-Kontakt (number == eigene
 *      LID-Ziffern ODER number leer bei gesetzter lid) und GENAU EIN echter
 *      Kontakt mit identischem Namen (trim/lower, nicht-numerisch, nicht leer)
 *      und plausibler Nummer (8-15 Ziffern). Bei mehreren Kandidaten wird
 *      NICHT gemerged, nur berichtet.
 *
 * WICHTIG: Standard ist DRY-RUN - es wird nur berichtet, nichts geschrieben.
 *
 * Ausfuehrung (lokal gegen die Railway-DB oder per `railway run`):
 *   DB_HOST=... DB_PORT=... DB_USER=... DB_PASS=... DB_NAME=... \
 *     node scripts/repair-lid-duplicates.js            # Dry-Run, Regel A
 *   ... node scripts/repair-lid-duplicates.js --names  # Dry-Run, Regel A+B
 *   ... node scripts/repair-lid-duplicates.js --apply         # schreibt Regel A
 *   ... node scripts/repair-lid-duplicates.js --apply --names # schreibt A+B
 *
 * Die Env-Variablen sind dieselben, die das Backend nutzt (src/config/database.ts).
 */

const mysql = require("mysql2/promise");

const APPLY = process.argv.includes("--apply");
const USE_NAMES = process.argv.includes("--names");

const lidDigits = lid => (lid || "").split("@")[0];

async function main() {
  const required = ["DB_HOST", "DB_USER", "DB_NAME"];
  const missing = required.filter(k => !process.env[k]);
  if (missing.length) {
    console.error(`Fehlende Env-Variablen: ${missing.join(", ")}`);
    console.error(
      "Benoetigt: DB_HOST, DB_USER, DB_PASS, DB_NAME (optional DB_PORT)"
    );
    process.exit(1);
  }

  const conn = await mysql.createConnection({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT || 3306),
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
    charset: "utf8mb4"
  });

  console.log(
    `Modus: ${APPLY ? "APPLY (schreibt!)" : "DRY-RUN (nur Bericht)"}` +
      `${USE_NAMES ? " + Name-Heuristik" : ""}`
  );

  // ---------- Regel A: deterministische Paare ueber lid <-> number ----------
  const [ruleA] = await conn.query(
    `SELECT p.id  AS pseudoId, p.name AS pseudoName, p.number AS pseudoNumber,
            p.lid AS pseudoLid,
            r.id  AS realId,   r.name AS realName,   r.number AS realNumber,
            r.lid AS realLid
       FROM Contacts p
       JOIN Contacts r
         ON r.id <> p.id
        AND r.lid IS NOT NULL AND r.lid <> ''
        AND p.number IS NOT NULL AND p.number <> ''
        AND SUBSTRING_INDEX(r.lid, '@', 1) = p.number
      WHERE p.isGroup = 0 AND r.isGroup = 0`
  );

  // ---------- Regel B: Name-Heuristik ----------
  let ruleB = [];
  if (USE_NAMES) {
    // Pseudo-Kontakte: Nummer == eigene LID-Ziffern ODER (keine Nummer, aber lid)
    const [pseudos] = await conn.query(
      `SELECT id, name, number, lid
         FROM Contacts
        WHERE isGroup = 0
          AND lid IS NOT NULL AND lid <> ''
          AND (number IS NULL OR number = '' OR number = SUBSTRING_INDEX(lid, '@', 1))`
    );

    for (const p of pseudos) {
      const name = (p.name || "").trim();
      if (!name || /^[0-9+ ()-]*$/.test(name)) continue; // numerischer/leerer Name: zu unsicher

      const [candidates] = await conn.query(
        `SELECT id, name, number, lid
           FROM Contacts
          WHERE isGroup = 0
            AND id <> ?
            AND number IS NOT NULL
            AND number REGEXP '^[0-9]{8,15}$'
            AND (lid IS NULL OR lid = '' OR SUBSTRING_INDEX(lid,'@',1) <> number)
            AND LOWER(TRIM(name)) = LOWER(?)
            AND (number <> ? )`,
        [p.id, name, lidDigits(p.lid) || "-"]
      );

      if (candidates.length === 1) {
        ruleB.push({
          pseudoId: p.id,
          pseudoName: p.name,
          pseudoNumber: p.number,
          pseudoLid: p.lid,
          realId: candidates[0].id,
          realName: candidates[0].name,
          realNumber: candidates[0].number,
          realLid: candidates[0].lid
        });
      } else if (candidates.length > 1) {
        console.log(
          `  [B/ambiguous] Pseudo #${p.id} "${p.name}" (${p.number || p.lid}): ` +
            `${candidates.length} Namens-Kandidaten -> NICHT gemerged: ` +
            candidates.map(c => `#${c.id} +${c.number}`).join(", ")
        );
      }
    }
  }

  // Regel-A-Paare haben Vorrang; B-Paare, deren pseudoId schon in A steckt, raus.
  const seen = new Set(ruleA.map(x => x.pseudoId));
  ruleB = ruleB.filter(x => !seen.has(x.pseudoId));

  const pairs = [
    ...ruleA.map(x => ({ ...x, rule: "A" })),
    ...ruleB.map(x => ({ ...x, rule: "B" }))
  ];

  if (pairs.length === 0) {
    console.log("Keine LID-Doubletten gefunden. Nichts zu tun.");
  }

  let merged = 0;
  for (const pair of pairs) {
    const label =
      `[${pair.rule}] Pseudo #${pair.pseudoId} "${pair.pseudoName}" ` +
      `(number=${pair.pseudoNumber || "-"}, lid=${pair.pseudoLid || "-"}) ` +
      `-> Echt #${pair.realId} "${pair.realName}" (+${pair.realNumber})`;

    const [[{ t }]] = await conn.query(
      "SELECT COUNT(*) t FROM Tickets  WHERE contactId = ?",
      [pair.pseudoId]
    );
    const [[{ m }]] = await conn.query(
      "SELECT COUNT(*) m FROM Messages WHERE contactId = ?",
      [pair.pseudoId]
    );

    console.log(`${label}  |  ${t} Ticket(s), ${m} Message(s) umzuhaengen`);

    if (!APPLY) continue;

    await conn.beginTransaction();
    try {
      await conn.query("UPDATE Tickets  SET contactId = ? WHERE contactId = ?", [
        pair.realId,
        pair.pseudoId
      ]);
      await conn.query("UPDATE Messages SET contactId = ? WHERE contactId = ?", [
        pair.realId,
        pair.pseudoId
      ]);

      const lidToKeep =
        pair.realLid ||
        pair.pseudoLid ||
        (pair.pseudoNumber ? `${pair.pseudoNumber}@lid` : null);

      // Pseudo zuerst loeschen (lid ist UNIQUE), dann lid am echten Kontakt nachtragen
      await conn.query("DELETE FROM Contacts WHERE id = ?", [pair.pseudoId]);

      if (lidToKeep && !pair.realLid) {
        await conn.query(
          "UPDATE Contacts SET lid = ?, updatedAt = NOW() WHERE id = ?",
          [lidToKeep, pair.realId]
        );
      }

      await conn.commit();
      merged += 1;
      console.log("   -> gemerged.");
    } catch (err) {
      await conn.rollback();
      console.error(`   -> FEHLER, Rollback fuer Paar ${label}:`, err.message);
    }
  }

  console.log(
    APPLY
      ? `Fertig: ${merged}/${pairs.length} Paar(e) gemerged.`
      : `Dry-Run beendet: ${pairs.length} Paar(e) wuerden gemerged (mit --apply ausfuehren).`
  );

  await conn.end();
}

main().catch(err => {
  console.error("Abbruch:", err);
  process.exit(1);
});
