ALTER TABLE telegram_index_folders
    ADD COLUMN IF NOT EXISTS parent_id UUID
        REFERENCES telegram_index_folders(id)
        ON DELETE CASCADE;

ALTER TABLE telegram_index_folders
    ADD COLUMN IF NOT EXISTS telegram_topic_id BIGINT NOT NULL DEFAULT 0;

ALTER TABLE telegram_index_folders
    ADD COLUMN IF NOT EXISTS is_forum BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE telegram_index_files
    ADD COLUMN IF NOT EXISTS telegram_topic_id BIGINT NOT NULL DEFAULT 0;

ALTER TABLE telegram_index_cursors
    ADD COLUMN IF NOT EXISTS structure_version INTEGER NOT NULL DEFAULT 1;

ALTER TABLE index_runs
    ADD COLUMN IF NOT EXISTS topics_seen INTEGER NOT NULL DEFAULT 0;

ALTER TABLE telegram_index_folders
    DROP CONSTRAINT IF EXISTS telegram_index_folders_user_id_telegram_peer_id_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_telegram_index_folders_peer_topic
    ON telegram_index_folders(
        user_id,
        telegram_peer_id,
        telegram_topic_id
    );

CREATE INDEX IF NOT EXISTS idx_telegram_index_folders_parent
    ON telegram_index_folders(parent_id, name);

CREATE INDEX IF NOT EXISTS idx_telegram_index_folders_topic
    ON telegram_index_folders(
        user_id,
        telegram_peer_id,
        telegram_topic_id
    );

CREATE INDEX IF NOT EXISTS idx_telegram_index_files_topic
    ON telegram_index_files(
        user_id,
        telegram_peer_id,
        telegram_topic_id
    );