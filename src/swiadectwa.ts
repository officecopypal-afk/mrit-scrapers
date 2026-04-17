import {
  MritDwrClient,
  SWIADECTWA_CFG,
  parseDwrTable,
  buildColumnMap,
  sleep,
} from './mrit-dwr.ts';
import { createDbPool, tableName, IS_SHADOW_MODE } from './db.ts';

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const PAGE_DELAY_MS = 1000;
const DUPLICATES_PAGE_DELAY_MS = 150;
const MIN_COLUMNS_IN_ROW = 14;
const STOP_AFTER_CONSECUTIVE_DUPLICATES = 150;
const UOZE_OVERFLOW_THRESHOLD = 150;
const DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;
const TABLE = tableName('swiadectwa_energetyczne');

interface PageResult {
  inserted: number;
  duplicates: number;
  crashed: boolean;
}

async function main() {
  console.log(`🤖 START scraper świadectw | shadow=${IS_SHADOW_MODE} | table=${TABLE}`);

  const client = new MritDwrClient(SWIADECTWA_CFG);
  const db = createDbPool();

  try {
    await client.initSession();
    console.log('✅ Sesja zainicjowana');

    let pageNum = 1;
    let consecutiveDuplicates = 0;
    let totalInserted = 0;
    let stopReason: 'aktualne' | 'awaria' = 'awaria';

    while (true) {
      const result = await processPage(client, db, pageNum);

      if (result.crashed) {
        console.log(`❌ CRASH na stronie ${pageNum}`);
        break;
      }

      if (result.inserted > 0) {
        consecutiveDuplicates = 0;
      } else {
        consecutiveDuplicates += result.duplicates;
      }

      totalInserted += result.inserted;

      console.log(
        `✔ Strona ${pageNum} — nowych: ${result.inserted}, dubli: ${result.duplicates}, seria: ${consecutiveDuplicates}/${STOP_AFTER_CONSECUTIVE_DUPLICATES}`,
      );

      if (consecutiveDuplicates >= STOP_AFTER_CONSECUTIVE_DUPLICATES) {
        console.log(`🛑 Stop: ${STOP_AFTER_CONSECUTIVE_DUPLICATES} dubli z rzędu — baza aktualna`);
        stopReason = 'aktualne';
        break;
      }

      pageNum++;
      await sleep(result.inserted > 0 ? PAGE_DELAY_MS : DUPLICATES_PAGE_DELAY_MS);
    }

    console.log(`✨ KONIEC — nowych łącznie: ${totalInserted}, reason: ${stopReason}`);
    if (stopReason === 'awaria') process.exitCode = 1;
  } finally {
    await db.end();
  }
}

async function processPage(
  client: MritDwrClient,
  db: ReturnType<typeof createDbPool>,
  pageNum: number,
): Promise<PageResult> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = await client.goToPage(pageNum);
      const { headers, rows } = parseDwrTable(response, 2000);

      if (rows.length === 0) {
        if (!response.includes('<table')) {
          throw new Error('Brak tabeli w odpowiedzi');
        }
        throw new Error('Serwer MRiT zadławił się (pusta tabela)');
      }

      const colMap = buildColumnMap(headers, {
        numer_sche: (h) => h.includes('numer świadectwa'),
        data: (h) => h.includes('data wystawienia'),
        wazne_do: (h) => h.includes('ważne do'),
        miejscowosc: (h) => h.includes('miejscowość'),
        ulica: (h) => h.includes('ulica'),
        nr_domu: (h) => h.includes('nr domu'),
        nr_lokalu: (h) => h.includes('nr lokalu'),
        woj: (h) => h.includes('województwo'),
        powiat: (h) => h.includes('powiat'),
        gmina: (h) => h.includes('gmina'),
        eu: (h) => h.includes('użytkową eu') || h.includes('eu ['),
        ek: (h) => h.includes('końcową ek') || h.includes('ek ['),
        ep: (h) => h.includes('pierwotną ep') || h.includes('ep ['),
        uoze: (h) => h.includes('uoze'),
        co2: (h) => h.includes('co2'),
      });

      const candidates: Array<(string | number | null)[]> = [];

      for (const row of rows) {
        if (row.length < MIN_COLUMNS_IN_ROW) continue;

        const numerSche = get(row, colMap, 'numer_sche');
        const dataWystawienia = get(row, colMap, 'data');
        if (!numerSche || !DATE_REGEX.test(dataWystawienia)) continue;

        const wazneDo = get(row, colMap, 'wazne_do');
        const miejscowosc = get(row, colMap, 'miejscowosc');
        const ulica = get(row, colMap, 'ulica');
        const nrBudynku = get(row, colMap, 'nr_domu');
        const nrLokalu = get(row, colMap, 'nr_lokalu');
        const wojewodztwo = get(row, colMap, 'woj');
        const powiat = get(row, colMap, 'powiat');
        const gmina = get(row, colMap, 'gmina');

        let uozeStr = get(row, colMap, 'uoze');
        let co2Str = get(row, colMap, 'co2');

        const uozeNum = parseNumber(uozeStr);
        if (uozeNum !== null && uozeNum > UOZE_OVERFLOW_THRESHOLD) {
          co2Str = uozeStr;
          uozeStr = '0.00';
        }

        const addressParts: string[] = [];
        if (ulica) addressParts.push(ulica);
        let numeration = nrBudynku;
        if (nrLokalu) numeration += `/${nrLokalu}`;
        if (numeration) addressParts.push(numeration);
        const streetPart = addressParts.join(' ');
        const adresCaly = [streetPart, miejscowosc].filter(Boolean).join(', ');

        candidates.push([
          numerSche,
          dataWystawienia,
          DATE_REGEX.test(wazneDo) ? wazneDo : null,
          miejscowosc,
          ulica,
          nrBudynku,
          nrLokalu,
          wojewodztwo,
          powiat,
          gmina,
          adresCaly,
          parseNumber(get(row, colMap, 'eu')),
          parseNumber(get(row, colMap, 'ek')),
          parseNumber(get(row, colMap, 'ep')),
          parseNumber(uozeStr),
          parseNumber(co2Str),
        ]);
      }

      let inserted = 0;
      if (candidates.length > 0) {
        const cols = 16;
        const values: string[] = [];
        const params: (string | number | null)[] = [];
        candidates.forEach((tuple, i) => {
          const base = i * cols;
          const placeholders = Array.from({ length: cols }, (_, j) => `$${base + j + 1}`);
          values.push(`(${placeholders.join(', ')})`);
          params.push(...tuple);
        });
        const result = await db.query(
          `INSERT INTO ${TABLE} (
             numer_swiadectwa, data_wystawienia, wazne_do, miejscowosc, ulica,
             nr_domu, nr_lokalu, wojewodztwo, powiat, gmina, adres_caly,
             wskaznik_eu, wskaznik_ek, wskaznik_ep, udzial_oze, emisja_co2
           ) VALUES ${values.join(', ')}
           ON CONFLICT (numer_swiadectwa) DO NOTHING
           RETURNING numer_swiadectwa`,
          params,
        );
        inserted = result.rowCount ?? 0;
      }
      const duplicates = candidates.length - inserted;

      return { inserted, duplicates, crashed: false };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.log(`⚠️ Błąd na str. ${pageNum} (próba ${attempt}/${MAX_RETRIES}): ${msg}`);
      if (attempt === MAX_RETRIES) return { inserted: 0, duplicates: 0, crashed: true };
      await sleep(RETRY_DELAY_MS);
    }
  }
  return { inserted: 0, duplicates: 0, crashed: true };
}

function get(row: string[], colMap: Record<string, number>, key: string): string {
  const idx = colMap[key];
  if (idx === undefined || idx < 0 || idx >= row.length) return '';
  return row[idx] ?? '';
}

function parseNumber(value: string | null | undefined): number | null {
  if (value === null || value === undefined) return null;
  const trimmed = String(value).trim();
  if (trimmed === '' || trimmed === '-') return null;
  const cleaned = trimmed.replace(/[\s\u00A0]/g, '').replace(/&nbsp;/g, '').replace(',', '.');
  const num = parseFloat(cleaned);
  return Number.isFinite(num) ? num : null;
}

main().catch((err) => {
  console.error('💥 Fatal:', err);
  process.exit(1);
});
