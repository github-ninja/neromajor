-- Move to timezone-aware timestamps and message-anchored auditing.
--
-- * All naive TIMESTAMP columns hold UTC (the server runs with timezone=UTC);
--   they become TIMESTAMPTZ.
-- * violation_logs -> violations: each violation now points at the message
--   that caused it and is dated by that message, not by the audit run.
-- * stats_checkpoint (a timestamp) -> chats.audit_cursor (last audited
--   messages.id), so a backlog is audited in batches without gaps.

-- users ---------------------------------------------------------------------

UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL;
ALTER TABLE users
    ALTER COLUMN updated_at TYPE TIMESTAMPTZ USING updated_at AT TIME ZONE 'UTC',
    ALTER COLUMN updated_at SET DEFAULT now(),
    ALTER COLUMN updated_at SET NOT NULL;
CREATE INDEX users_username_lower_idx ON users (lower(username));

-- messages ------------------------------------------------------------------

DELETE FROM messages WHERE chat_id IS NULL OR user_id IS NULL;
ALTER TABLE messages RENAME COLUMN "timestamp" TO created_at;
UPDATE messages SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL;
UPDATE messages SET content = '' WHERE content IS NULL;
ALTER TABLE messages
    ALTER COLUMN chat_id SET NOT NULL,
    ALTER COLUMN user_id SET NOT NULL,
    ALTER COLUMN content SET NOT NULL,
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN created_at SET DEFAULT now(),
    ALTER COLUMN created_at SET NOT NULL,
    ADD COLUMN tg_message_id BIGINT;

DROP INDEX IF EXISTS idx_messages_chat_ts;
DROP INDEX IF EXISTS idx_messages_chat_timestamp;
DROP INDEX IF EXISTS idx_messages_user_id;
CREATE INDEX messages_chat_id_idx      ON messages (chat_id, id);
CREATE INDEX messages_chat_created_idx ON messages (chat_id, created_at);
CREATE INDEX messages_chat_user_idx    ON messages (chat_id, user_id, id);

-- violations ----------------------------------------------------------------

CREATE TABLE violations (
    id           BIGSERIAL PRIMARY KEY,
    chat_id      BIGINT NOT NULL,
    user_id      BIGINT NOT NULL,
    message_id   INTEGER REFERENCES messages (id) ON DELETE SET NULL,
    content      TEXT NOT NULL,
    article      TEXT NOT NULL DEFAULT '',
    fines        BIGINT NOT NULL DEFAULT 0 CHECK (fines >= 0),
    days         INTEGER NOT NULL DEFAULT 0 CHECK (days >= 0),
    committed_at TIMESTAMPTZ NOT NULL,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX violations_message_uq ON violations (message_id) WHERE message_id IS NOT NULL;
CREATE INDEX violations_chat_user_idx ON violations (chat_id, user_id, committed_at);

-- Legacy rows stored only the offending text and the audit time. Recover the
-- source message (same chat, author and text, written before the audit) so
-- the violation gets its real date; fall back to the audit time otherwise.
INSERT INTO violations (chat_id, user_id, message_id, content, article, fines, days,
                        committed_at, detected_at)
SELECT v.chat_id,
       v.user_id,
       m.id,
       COALESCE(v.content, ''),
       COALESCE(v.article, ''),
       GREATEST(COALESCE(v.fines, 0), 0),
       GREATEST(COALESCE(v.days, 0), 0),
       COALESCE(m.created_at, v.created_at AT TIME ZONE 'UTC', now()),
       COALESCE(v.created_at AT TIME ZONE 'UTC', now())
FROM violation_logs v
LEFT JOIN LATERAL (
    SELECT mm.id, mm.created_at
    FROM messages mm
    WHERE mm.chat_id = v.chat_id
      AND mm.user_id = v.user_id
      AND mm.content = v.content
      AND NOT mm.is_bot
      AND mm.created_at <= COALESCE(v.created_at AT TIME ZONE 'UTC', now())
    ORDER BY mm.id DESC
    LIMIT 1
) m ON TRUE
WHERE v.chat_id IS NOT NULL AND v.user_id IS NOT NULL
ORDER BY v.id
ON CONFLICT (message_id) WHERE message_id IS NOT NULL DO NOTHING;

DROP TABLE violation_logs;

-- chats ---------------------------------------------------------------------

CREATE TABLE chats (
    chat_id           BIGINT PRIMARY KEY,
    audit_cursor      INTEGER NOT NULL DEFAULT 0,
    last_audit_at     TIMESTAMPTZ,
    last_proactive_on DATE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A checkpoint meant "everything written before last_check was audited".
INSERT INTO chats (chat_id, audit_cursor, last_audit_at)
SELECT c.chat_id,
       COALESCE((SELECT max(m.id) FROM messages m
                 WHERE m.chat_id = c.chat_id
                   AND m.created_at <= c.last_check AT TIME ZONE 'UTC'), 0),
       c.last_check AT TIME ZONE 'UTC'
FROM stats_checkpoint c;

INSERT INTO chats (chat_id)
SELECT DISTINCT chat_id FROM messages
ON CONFLICT (chat_id) DO NOTHING;

DROP TABLE stats_checkpoint;
