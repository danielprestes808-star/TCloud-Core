-- Pareamento seguro de clientes sem distribuir o token mestre do Core.
CREATE TABLE IF NOT EXISTS device_pairing_codes (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    code_hash TEXT NOT NULL UNIQUE,
    device_name TEXT NOT NULL,
    platform TEXT NOT NULL,
    app_version TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_device_pairing_codes_active
    ON device_pairing_codes(expires_at)
    WHERE used_at IS NULL;
