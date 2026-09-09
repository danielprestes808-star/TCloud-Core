CREATE UNIQUE INDEX IF NOT EXISTS users_username_unique_ci
    ON users (LOWER(username))
    WHERE username IS NOT NULL AND BTRIM(username) <> '';

