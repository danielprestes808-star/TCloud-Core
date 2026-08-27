CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    telegram_user_id BIGINT UNIQUE,
    display_name TEXT NOT NULL,
    username TEXT,
    avatar_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS devices (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    platform TEXT NOT NULL,
    app_version TEXT,
    device_key TEXT UNIQUE,
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sessions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id UUID REFERENCES devices(id) ON DELETE SET NULL,
    token_hash TEXT NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS folders (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    parent_id UUID REFERENCES folders(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    telegram_chat_id BIGINT,
    telegram_topic_id BIGINT,
    revision BIGINT NOT NULL DEFAULT 1,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS files (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    parent_id UUID REFERENCES folders(id) ON DELETE SET NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'file',
    size_bytes BIGINT NOT NULL DEFAULT 0,
    mime TEXT NOT NULL DEFAULT 'application/octet-stream',
    content_hash TEXT,
    telegram_chat_id BIGINT,
    telegram_message_id BIGINT,
    telegram_file_id TEXT,
    revision BIGINT NOT NULL DEFAULT 1,
    source TEXT NOT NULL DEFAULT 'telegram',
    sync_state TEXT NOT NULL DEFAULT 'online',
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS file_versions (
    id UUID PRIMARY KEY,
    file_id UUID NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    revision BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    content_hash TEXT,
    telegram_chat_id BIGINT,
    telegram_message_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(file_id, revision)
);

CREATE TABLE IF NOT EXISTS favorites (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    file_id UUID NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(user_id, file_id)
);

CREATE TABLE IF NOT EXISTS device_file_state (
    device_id UUID NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    file_id UUID NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    pinned BOOLEAN NOT NULL DEFAULT FALSE,
    hydrated BOOLEAN NOT NULL DEFAULT FALSE,
    last_hydrated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(device_id, file_id)
);

CREATE TABLE IF NOT EXISTS sync_events (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL,
    entity_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    revision BIGINT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sync_queue (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id UUID REFERENCES devices(id) ON DELETE CASCADE,
    file_id UUID REFERENCES files(id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_devices_user
    ON devices(user_id);

CREATE INDEX IF NOT EXISTS idx_sessions_user_active
    ON sessions(user_id, expires_at)
    WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_folders_user_parent
    ON folders(user_id, parent_id)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_files_user_parent
    ON files(user_id, parent_id)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_files_telegram_message
    ON files(telegram_chat_id, telegram_message_id)
    WHERE telegram_message_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sync_events_user_id
    ON sync_events(user_id, id);

CREATE INDEX IF NOT EXISTS idx_sync_queue_pending
    ON sync_queue(status, available_at)
    WHERE status = 'pending';