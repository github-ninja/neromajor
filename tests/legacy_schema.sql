-- Legacy schema exactly as the pre-refactor bot created it in production
-- (naive TIMESTAMP columns holding UTC). Every statement is idempotent so
-- this is a no-op on the existing database and a starting point for a fresh
-- one; 0002 then converts both into the current schema.

CREATE TABLE IF NOT EXISTS users (
    user_id      BIGINT PRIMARY KEY,
    username     TEXT,
    first_name   TEXT,
    last_name    TEXT,
    display_name TEXT NOT NULL,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS messages (
    id          SERIAL PRIMARY KEY,
    chat_id     BIGINT,
    user_id     BIGINT,
    content     TEXT,
    is_bot      BOOLEAN NOT NULL DEFAULT FALSE,
    "timestamp" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS violation_logs (
    id              SERIAL PRIMARY KEY,
    chat_id         BIGINT,
    user_id         BIGINT,
    content         TEXT,
    article         TEXT,
    fines           BIGINT DEFAULT 0,
    days            BIGINT DEFAULT 0,
    violation_count BIGINT DEFAULT 1,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stats_checkpoint (
    chat_id    BIGINT PRIMARY KEY,
    last_check TIMESTAMP
);
