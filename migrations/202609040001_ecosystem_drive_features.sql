-- TCloud Ecosystem 8.0 - shared Drive-like capabilities.

CREATE TABLE IF NOT EXISTS tcloud_favorites (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    file_id UUID NOT NULL REFERENCES telegram_index_files(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, file_id)
);

CREATE TABLE IF NOT EXISTS tcloud_activity (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    file_id UUID REFERENCES telegram_index_files(id) ON DELETE SET NULL,
    device_id UUID REFERENCES devices(id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    detail TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tcloud_activity_user_created
    ON tcloud_activity(user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS tcloud_share_links (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    file_id UUID NOT NULL REFERENCES telegram_index_files(id) ON DELETE CASCADE,
    token UUID NOT NULL UNIQUE,
    password_hash TEXT,
    expires_at TIMESTAMPTZ,
    max_downloads INTEGER,
    download_count INTEGER NOT NULL DEFAULT 0,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (max_downloads IS NULL OR max_downloads > 0)
);

CREATE INDEX IF NOT EXISTS idx_tcloud_share_links_file
    ON tcloud_share_links(user_id, file_id, created_at DESC);

