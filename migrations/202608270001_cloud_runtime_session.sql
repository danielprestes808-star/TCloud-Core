-- TCLOUD_CLOUD_RUNTIME_100
CREATE SCHEMA IF NOT EXISTS tcloud_private;

CREATE TABLE IF NOT EXISTS tcloud_private.core_runtime_sessions (
    session_key TEXT PRIMARY KEY,
    encrypted_payload BYTEA NOT NULL,
    payload_bytes BIGINT NOT NULL CHECK (payload_bytes >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE tcloud_private.core_runtime_sessions IS
    'Encrypted runtime session snapshots for TCloud Core.';
