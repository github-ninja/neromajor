"""Telegram presentation helpers: HTML escaping, names, message-size splitting."""

import html

from aiogram import types

TG_LIMIT = 4096


def escape(text: object) -> str:
    return html.escape(str(text), quote=False)


def display_name(user: types.User) -> str:
    full = " ".join(filter(None, [user.first_name, user.last_name])).strip()
    return full or user.username or f"ID{user.id}"


def split_text(text: str, limit: int) -> list[str]:
    """Split plain text into pieces of at most *limit* chars, preferring paragraph/line breaks."""
    pieces: list[str] = []
    rest = text.strip()
    while len(rest) > limit:
        cut = -1
        for sep in ("\n\n", "\n", ". ", " "):
            cut = rest.rfind(sep, 0, limit)
            if cut > limit // 2:
                cut += len(sep)
                break
        if cut <= 0:
            cut = limit
        pieces.append(rest[:cut].rstrip())
        rest = rest[cut:].lstrip()
    if rest:
        pieces.append(rest)
    return pieces


def pack_blocks(blocks: list[str], limit: int = TG_LIMIT, sep: str = "\n") -> list[str]:
    """
    Join pre-rendered HTML blocks into as few messages as possible, never
    splitting inside a block. A block must itself fit into *limit*.
    """
    messages: list[str] = []
    current = ""
    for block in blocks:
        candidate = f"{current}{sep}{block}" if current else block
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            messages.append(current)
        current = block[:limit]
    if current:
        messages.append(current)
    return messages


def plain_to_html_messages(header_html: str, body: str, limit: int = TG_LIMIT) -> list[str]:
    """Header (HTML) + plain-text body escaped and split to fit Telegram's limit."""
    # Escaping can only grow text (&, <, >), so split with headroom and verify.
    budget = limit - len(header_html) - 64
    chunks = split_text(body, max(500, budget * 3 // 4))
    messages = [escape(c) for c in chunks] or [""]
    messages[0] = header_html + messages[0]
    return [m[:limit] for m in messages]
