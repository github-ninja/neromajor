from bot.tg import TG_LIMIT, pack_blocks, plain_to_html_messages, split_text


def test_split_text_respects_limit_and_keeps_content():
    text = "\n\n".join(f"Абзац {i}. " + "слово " * 80 for i in range(40))
    pieces = split_text(text, 1000)
    assert all(len(p) <= 1000 for p in pieces)
    assert "".join(pieces).replace(" ", "").replace("\n", "") == text.replace(" ", "").replace("\n", "")


def test_split_text_hard_cut_without_separators():
    assert [len(p) for p in split_text("x" * 2500, 1000)] == [1000, 1000, 500]


def test_pack_blocks_never_splits_a_block():
    blocks = [f"<b>block {i}</b> " + "y" * 900 for i in range(10)]
    messages = pack_blocks(blocks, limit=TG_LIMIT)
    assert all(len(m) <= TG_LIMIT for m in messages)
    assert sum(m.count("<b>block") for m in messages) == 10
    assert len(messages) == 3


def test_plain_to_html_messages_escapes_and_fits():
    body = "<script> & " * 2000
    messages = plain_to_html_messages("<b>H</b>\n", body)
    assert messages[0].startswith("<b>H</b>\n")
    assert all(len(m) <= TG_LIMIT for m in messages)
    assert "<script>" not in "".join(messages)
