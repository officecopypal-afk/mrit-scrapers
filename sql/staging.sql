-- Tabele shadow mode — identyczne struktury jak produkcyjne, oddzielne dane.
-- Uruchom w Supabase SQL Editor przed pierwszym shadow runem.

CREATE TABLE IF NOT EXISTS inzynierowie_staging
  (LIKE inzynierowie INCLUDING ALL);

CREATE TABLE IF NOT EXISTS swiadectwa_energetyczne_staging
  (LIKE swiadectwa_energetyczne INCLUDING ALL);
