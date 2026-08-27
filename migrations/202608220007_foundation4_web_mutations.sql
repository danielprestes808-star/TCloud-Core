ALTER TABLE telegram_index_files
    ADD COLUMN IF NOT EXISTS original_parent_id UUID
        REFERENCES telegram_index_folders(id)
        ON DELETE SET NULL;

ALTER TABLE telegram_index_files
    ADD COLUMN IF NOT EXISTS trashed_at TIMESTAMPTZ;

ALTER TABLE telegram_index_files
    ADD COLUMN IF NOT EXISTS manual_parent_override BOOLEAN
        NOT NULL DEFAULT FALSE;

ALTER TABLE telegram_index_files
    ADD COLUMN IF NOT EXISTS manual_trash BOOLEAN
        NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_telegram_index_files_manual_trash
    ON telegram_index_files(user_id, manual_trash, trashed_at DESC);

CREATE INDEX IF NOT EXISTS idx_telegram_index_files_manual_parent
    ON telegram_index_files(user_id, manual_parent_override, parent_id);