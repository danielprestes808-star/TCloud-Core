CREATE TABLE IF NOT EXISTS telegram_accounts (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    telegram_user_id BIGINT UNIQUE,
    phone_hint TEXT,
    display_name TEXT,
    username TEXT,
    authorized BOOLEAN NOT NULL DEFAULT FALSE,
    session_storage TEXT NOT NULL DEFAULT 'grammers',
    last_authorized_at TIMESTAMPTZ,
    last_indexed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS auth_challenges (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    phone_hint TEXT,
    provider TEXT NOT NULL DEFAULT 'telegram',
    status TEXT NOT NULL DEFAULT 'created',
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS telegram_dialogs (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    telegram_peer_id BIGINT NOT NULL,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    username TEXT,
    is_forum BOOLEAN NOT NULL DEFAULT FALSE,
    access_hash TEXT,
    last_message_id BIGINT,
    last_indexed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, telegram_peer_id)
);

CREATE TABLE IF NOT EXISTS index_runs (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    provider TEXT NOT NULL DEFAULT 'telegram',
    mode TEXT NOT NULL DEFAULT 'delta',
    status TEXT NOT NULL DEFAULT 'queued',
    dialogs_seen INTEGER NOT NULL DEFAULT 0,
    messages_seen BIGINT NOT NULL DEFAULT 0,
    files_upserted BIGINT NOT NULL DEFAULT 0,
    error TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_telegram_accounts_user
    ON telegram_accounts(user_id);

CREATE INDEX IF NOT EXISTS idx_auth_challenges_status
    ON auth_challenges(status, expires_at);

CREATE INDEX IF NOT EXISTS idx_telegram_dialogs_user
    ON telegram_dialogs(user_id, telegram_peer_id);

CREATE INDEX IF NOT EXISTS idx_index_runs_status
    ON index_runs(status, created_at);