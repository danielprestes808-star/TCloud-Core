INSERT INTO users (
    id,
    telegram_user_id,
    display_name,
    username
)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    NULL,
    'TCloud Local',
    'local-dev'
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO devices (
    id,
    user_id,
    name,
    platform,
    app_version,
    device_key,
    last_seen_at
)
VALUES (
    '00000000-0000-0000-0000-000000000002',
    '00000000-0000-0000-0000-000000000001',
    'Navegador local',
    'web',
    'foundation-2',
    'tcloud-local-web',
    NOW()
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO folders (
    id,
    user_id,
    parent_id,
    name,
    revision
)
VALUES
(
    '00000000-0000-0000-0000-000000001001',
    '00000000-0000-0000-0000-000000000001',
    NULL,
    'Documentos',
    1
),
(
    '00000000-0000-0000-0000-000000001002',
    '00000000-0000-0000-0000-000000000001',
    NULL,
    'Fotos',
    1
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO files (
    id,
    user_id,
    parent_id,
    name,
    kind,
    size_bytes,
    mime,
    revision,
    source,
    sync_state
)
VALUES
(
    '00000000-0000-0000-0000-000000002001',
    '00000000-0000-0000-0000-000000000001',
    NULL,
    'Relatorio TCloud.pdf',
    'pdf',
    2430000,
    'application/pdf',
    1,
    'demo',
    'device'
),
(
    '00000000-0000-0000-0000-000000002002',
    '00000000-0000-0000-0000-000000000001',
    NULL,
    'Apresentacao.mp4',
    'video',
    84200000,
    'video/mp4',
    1,
    'demo',
    'online'
),
(
    '00000000-0000-0000-0000-000000002003',
    '00000000-0000-0000-0000-000000000001',
    NULL,
    'Capa do projeto.jpg',
    'image',
    5120000,
    'image/jpeg',
    1,
    'demo',
    'syncing'
)
ON CONFLICT (id) DO NOTHING;