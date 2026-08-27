CREATE TABLE IF NOT EXISTS telegram_index_folders (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    telegram_peer_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'folder',
    source TEXT NOT NULL DEFAULT 'telegram',
    last_message_id BIGINT,
    last_indexed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    UNIQUE(user_id, telegram_peer_id)
);

CREATE TABLE IF NOT EXISTS telegram_index_files (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    parent_id UUID REFERENCES telegram_index_folders(id) ON DELETE CASCADE,
    telegram_peer_id BIGINT NOT NULL,
    telegram_message_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'file',
    size_bytes BIGINT NOT NULL DEFAULT 0,
    mime TEXT NOT NULL DEFAULT 'application/octet-stream',
    sync_state TEXT NOT NULL DEFAULT 'online',
    source TEXT NOT NULL DEFAULT 'telegram',
    message_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    UNIQUE(user_id, telegram_peer_id, telegram_message_id)
);

CREATE TABLE IF NOT EXISTS telegram_index_cursors (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    telegram_peer_id BIGINT NOT NULL,
    last_message_id BIGINT NOT NULL DEFAULT 0,
    last_indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(user_id, telegram_peer_id)
);

CREATE INDEX IF NOT EXISTS idx_telegram_index_folders_user
    ON telegram_index_folders(user_id, name);

CREATE INDEX IF NOT EXISTS idx_telegram_index_files_parent
    ON telegram_index_files(parent_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_telegram_index_files_remote
    ON telegram_index_files(user_id, telegram_peer_id, telegram_message_id);

CREATE INDEX IF NOT EXISTS idx_telegram_index_files_name
    ON telegram_index_files(user_id, name);

ALTER TABLE index_runs
    ADD COLUMN IF NOT EXISTS dialogs_updated INTEGER NOT NULL DEFAULT 0;

ALTER TABLE index_runs
    ADD COLUMN IF NOT EXISTS messages_seen BIGINT NOT NULL DEFAULT 0;

ALTER TABLE index_runs
    ADD COLUMN IF NOT EXISTS files_upserted BIGINT NOT NULL DEFAULT 0;