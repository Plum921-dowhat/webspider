"""Markdown-aware chunking for RAG ingestion.

Strategy: split on headings first (they carry document structure), then split
oversize sections on paragraph boundaries with a small tail overlap. Every
chunk keeps its heading path (e.g. ["Article title", "Top answer"]) so vector
search results can cite where they came from. Single oversize paragraphs (e.g.
code blocks) are never hard-split.
"""
import re

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*\S)\s*$")


def _parse_sections(text):
    """Yield (heading_path, body) for each heading-delimited section. Text
    before the first heading gets an empty path. The heading line stays glued
    to the body that follows it."""
    path = []
    sections = []
    current = []
    for line in text.splitlines():
        m = _HEADING_RE.match(line)
        if m:
            if current:
                sections.append((list(path), "\n".join(current).strip()))
                current = []
            level = len(m.group(1))
            path = path[: level - 1] + [m.group(2)]  # heading stack
            current = [line]
        else:
            current.append(line)
    if current:
        sections.append((list(path), "\n".join(current).strip()))
    return [(p, b) for p, b in sections if b]


def _split_paragraphs(paragraphs, target, overlap):
    """Split an oversize section's paragraphs into chunks of at most ~target
    chars, re-carrying trailing paragraphs (<= overlap chars) as the head of
    the next chunk."""
    chunks = []
    buf = []
    buf_len = 0
    for para in paragraphs:
        if buf and buf_len + len(para) + 2 > target:
            chunks.append("\n\n".join(buf))
            carry = []
            carry_len = 0
            for p in reversed(buf):
                if carry_len + len(p) + 2 > overlap:
                    break
                carry.insert(0, p)
                carry_len += len(p) + 2
            buf = carry
            buf_len = sum(len(p) for p in buf) + 2 * max(0, len(buf) - 1)
        buf.append(para)
        buf_len += len(para) + (2 if buf_len else 0)
    if buf:
        chunks.append("\n\n".join(buf))
    return chunks


def split_markdown(text, target=1200, overlap=150, min_chars=30):
    """Split markdown text into chunks. Returns a list of
    {"text": ..., "heading_path": [...]} dicts, in document order."""
    out = []
    for path, body in _parse_sections(text or ""):
        if len(body) <= target:
            if len(body) >= min_chars:
                out.append({"text": body, "heading_path": path})
            continue
        paragraphs = [p.strip() for p in re.split(r"\n\s*\n", body) if p.strip()]
        # glue the section's heading line(s) onto the first chunk
        prefix = ""
        while paragraphs and all(_HEADING_RE.match(l) for l in paragraphs[0].splitlines()):
            prefix = (prefix + "\n\n" + paragraphs.pop(0)).strip()
        for piece in _split_paragraphs(paragraphs, target, overlap):
            full = (prefix + "\n\n" + piece).strip() if prefix else piece
            if len(full) >= min_chars:
                out.append({"text": full, "heading_path": list(path)})
    return out
