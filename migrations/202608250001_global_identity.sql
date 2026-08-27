-- TCloud Foundation 7.0 - Global Identity
-- IDs UUID canônicos do Core passam a ser a identidade compartilhada entre todos os clientes.

ALTER TABLE telegram_index_files
    ADD COLUMN IF NOT EXISTS canonical_id UUID;

UPDATE telegram_index_files
SET canonical_id = id
WHERE canonical_id IS NULL;

ALTER TABLE telegram_index_files
    ALTER COLUMN canonical_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_telegram_index_files_canonical_id
    ON telegram_index_files(canonical_id);

ALTER TABLE telegram_index_folders
    ADD COLUMN IF NOT EXISTS canonical_id UUID;

UPDATE telegram_index_folders
SET canonical_id = id
WHERE canonical_id IS NULL;

ALTER TABLE telegram_index_folders
    ALTER COLUMN canonical_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_telegram_index_folders_canonical_id
    ON telegram_index_folders(canonical_id);

CREATE TABLE IF NOT EXISTS client_file_identity_map (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id UUID NOT NULL,
    canonical_file_id UUID NOT NULL REFERENCES telegram_index_files(canonical_id) ON DELETE CASCADE,
    local_record_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(user_id, device_id, canonical_file_id),
    UNIQUE(user_id, device_id, local_record_key)
);

CREATE TABLE IF NOT EXISTS client_folder_identity_map (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id UUID NOT NULL,
    canonical_folder_id UUID NOT NULL REFERENCES telegram_index_folders(canonical_id) ON DELETE CASCADE,
    local_record_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(user_id, device_id, canonical_folder_id),
    UNIQUE(user_id, device_id, local_record_key)
);