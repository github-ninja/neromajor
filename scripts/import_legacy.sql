-- One-off import of the pre-refactor database (2026-10-16).
--
-- Before: the old tables were moved aside with
--   ALTER SCHEMA public RENAME TO legacy; CREATE SCHEMA public;
-- and the new schema was created in public (python -m bot.db).
--
-- Users and the message history are carried over; violations are not
-- (amnesty). Imported messages are marked as audited, so the new auditor
-- only looks at messages written after the import.
-- Old timestamps are naive UTC.

BEGIN;

INSERT INTO public.users (user_id, username, first_name, last_name, display_name, updated_at)
SELECT user_id, username, first_name, last_name, display_name,
       COALESCE(updated_at AT TIME ZONE 'UTC', now())
FROM legacy.users;

-- Authors without a users row: the bot itself, plus any orphan just in case.
INSERT INTO public.users (user_id, display_name, is_bot)
SELECT user_id, CASE WHEN bool_or(is_bot) THEN 'Товарищ Майор' ELSE 'ID' || user_id END, bool_or(is_bot)
FROM legacy.messages
WHERE user_id IS NOT NULL
GROUP BY user_id
ON CONFLICT (user_id) DO NOTHING;

INSERT INTO public.chats (chat_id)
SELECT DISTINCT chat_id FROM legacy.messages WHERE chat_id IS NOT NULL;

INSERT INTO public.messages (chat_id, user_id, content, is_bot, created_at)
SELECT chat_id, user_id, COALESCE(content, ''), is_bot,
       COALESCE("timestamp" AT TIME ZONE 'UTC', now())
FROM legacy.messages
WHERE chat_id IS NOT NULL AND user_id IS NOT NULL
ORDER BY id;

UPDATE public.chats c
SET audit_cursor  = COALESCE((SELECT max(id) FROM public.messages m WHERE m.chat_id = c.chat_id), 0),
    last_audit_at = now();

COMMIT;
