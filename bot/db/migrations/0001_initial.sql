-- Initial schema. All timestamps are UTC (timestamptz); local time exists
-- only at the presentation layer.

CREATE TABLE users (
    user_id      BIGINT PRIMARY KEY,
    username     TEXT,
    first_name   TEXT,
    last_name    TEXT,
    display_name TEXT NOT NULL,
    is_bot       BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX users_username_idx ON users (lower(username));

CREATE TABLE chats (
    chat_id           BIGINT PRIMARY KEY,
    -- messages.id of the last audited message; everything after it is pending
    audit_cursor      BIGINT NOT NULL DEFAULT 0,
    last_audit_at     TIMESTAMPTZ,
    last_proactive_on DATE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Chat speech: human messages and the bot's own replies (is_bot).
CREATE TABLE messages (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chat_id       BIGINT NOT NULL REFERENCES chats (chat_id) ON DELETE CASCADE,
    user_id       BIGINT NOT NULL REFERENCES users (user_id),
    tg_message_id BIGINT,
    content       TEXT NOT NULL,
    is_bot        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX messages_chat_human_idx ON messages (chat_id, id) WHERE NOT is_bot;
CREATE INDEX messages_chat_bot_idx   ON messages (chat_id, id) WHERE is_bot;
CREATE INDEX messages_chat_user_idx  ON messages (chat_id, user_id, id);
CREATE INDEX messages_chat_time_idx  ON messages (chat_id, created_at);

-- One verdict per message; chat, author, text and date come from the message.
CREATE TABLE violations (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id  BIGINT NOT NULL UNIQUE REFERENCES messages (id) ON DELETE CASCADE,
    article     TEXT NOT NULL DEFAULT '',
    fines       BIGINT NOT NULL DEFAULT 0 CHECK (fines >= 0),
    days        INTEGER NOT NULL DEFAULT 0 CHECK (days >= 0),
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (fines > 0 OR days > 0)
);
