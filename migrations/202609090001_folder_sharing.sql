ALTER TABLE tcloud_share_links
    ALTER COLUMN file_id DROP NOT NULL;

ALTER TABLE tcloud_share_links
    ADD COLUMN IF NOT EXISTS folder_id UUID REFERENCES telegram_index_folders(id) ON DELETE CASCADE;

ALTER TABLE tcloud_share_links
    ADD CONSTRAINT tcloud_share_links_single_target
    CHECK ((file_id IS NOT NULL)::int + (folder_id IS NOT NULL)::int = 1);

CREATE INDEX IF NOT EXISTS idx_tcloud_share_links_folder
    ON tcloud_share_links(user_id, folder_id, created_at DESC)
    WHERE folder_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS tcloud_share_access (
    token UUID PRIMARY KEY,
    share_id UUID NOT NULL REFERENCES tcloud_share_links(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tcloud_share_access_expiry
    ON tcloud_share_access(expires_at);
