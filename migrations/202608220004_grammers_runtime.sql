CREATE UNIQUE INDEX IF NOT EXISTS idx_telegram_accounts_user_unique
    ON telegram_accounts(user_id);

ALTER TABLE telegram_accounts
    ADD COLUMN IF NOT EXISTS session_path TEXT;

ALTER TABLE telegram_accounts
    ADD COLUMN IF NOT EXISTS last_error TEXT;

ALTER TABLE index_runs
    ADD COLUMN IF NOT EXISTS dialogs_updated INTEGER NOT NULL DEFAULT 0;