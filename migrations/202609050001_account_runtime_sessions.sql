-- Isola snapshots Telegram por conta e migra a sessão única existente.
ALTER TABLE tcloud_private.core_runtime_sessions
    ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE CASCADE;

UPDATE tcloud_private.core_runtime_sessions
SET
    user_id = '00000000-0000-0000-0000-000000000001'::UUID,
    session_key = 'telegram-account:00000000-0000-0000-0000-000000000001'
WHERE user_id IS NULL;

ALTER TABLE tcloud_private.core_runtime_sessions
    ALTER COLUMN user_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_core_runtime_sessions_user
    ON tcloud_private.core_runtime_sessions(user_id);

COMMENT ON COLUMN tcloud_private.core_runtime_sessions.user_id IS
    'Conta proprietaria do snapshot Telegram criptografado.';
