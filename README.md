# MRiT Scrapers

Scrapery dwóch rejestrów Ministerstwa Rozwoju i Technologii → Supabase.

- **Inżynierowie** (`src/inzynierowie.ts`) — pełny przelot przez rejestr uprawnionych + usunięcie wykreślonych. Wolny (~15 min), jeden run na raz.
- **Świadectwa** (`src/swiadectwa.ts`) — pobieranie nowych świadectw energetycznych, stop po 150 dublach z rzędu. Szybki (~30–60s w zaktualizowanej bazie).

## Uruchomienie

- **Produkcja** — GitHub Actions, workflow `Scraper ...`, trigger ręczny (`workflow_dispatch`) lub cron (po odkomentowaniu w YAML).
- **Shadow mode** — ten sam workflow z inputem `mode=shadow`. Zapisuje do tabel `*_staging` zamiast produkcyjnych.

## Sekrety

W repozytorium → Settings → Secrets and variables → Actions:

- `SUPABASE_DB_URL` — connection string do pooler Supabase. Format:
  ```
  postgresql://postgres.<project-ref>:<password>@aws-1-eu-central-1.pooler.supabase.com:6543/postgres?sslmode=require
  ```

## Rozwój lokalny

```bash
npm install
export SUPABASE_DB_URL="postgresql://..."
export SHADOW_MODE=1
npm run scrape:swiadectwa
```

## Tabele w Supabase

Produkcyjne: `inzynierowie`, `swiadectwa_energetyczne` (już istnieją).

Shadow: `inzynierowie_staging`, `swiadectwa_energetyczne_staging` — trzeba utworzyć ze strukturą identyczną jak produkcyjne (patrz skrypt SQL w `sql/staging.sql`).

## Architektura

- `src/mrit-dwr.ts` — klient DWR (Direct Web Remoting) — stary protokół Spring/XAVA używany przez portal MRiT. Trzyma sesję w pamięci, buduje payloady, parsuje tabele HTML z odpowiedzi.
- `src/db.ts` — pula `pg` + przełącznik shadow mode.
- `src/inzynierowie.ts`, `src/swiadectwa.ts` — logika biznesowa per scraper.

## Migracja z PHP (seohost → GitHub Actions)

1. Shadow run — 2–3 razy ręcznie przez `workflow_dispatch` z `mode=shadow`
2. Diff: porównanie liczb rekordów i przykładowych wartości między `*_staging` a produkcyjnymi
3. Po akceptacji: uruchomienie w `mode=production` (stary PHP nadal leci równolegle)
4. Odkomentowanie `schedule:` w workflow YAML → commit → push
5. Po 48h bez rozjazdu — wyłączenie crona na seohost
