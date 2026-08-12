"""Quality gate: "wide in, strict out".

We do NOT reject solely on character count. Instead we combine:
  * code-block share (articles with a real ```python``` block are high value)
  * information density (lexical diversity, link dumping, spam/help phrases)

Thresholds live in config.py so they can be tuned via quality_probe.py.
"""
import re
from collections import Counter

from config import (
    MIN_CONTENT_LEN, MIN_CODE_CHARS, MAX_LINK_RATIO,
    MIN_UNIQUE_RATIO, MIN_WORDS_NO_CODE,
)

_FENCE_RE = re.compile(r"```[^\n]*\n(.*?)```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`[^`\n]+`")
_LINK_RE = re.compile(r"https?://\S+|\[[^\]]+\]\(https?://\S+\)")
_WORD_RE = re.compile(r"[A-Za-z][A-Za-z'-]+")

# phrases that indicate low-value / non-article content
_SPAM_PATTERNS = [
    r"\bplease\s+(help|subscribe|follow|upvote|donate)\b",
    r"\bcheck\s+out\s+my\s+(channel|blog|repo|github)\b",
    r"\b(free|discount|coupon|promo)\s+code\b",
    r"\bbuy\s+now\b",
    r"\bclick\s+here\b",
    r"\b(job opening|we are hiring|hiring now)\b",
]
_SPAM_RE = re.compile("|".join(_SPAM_PATTERNS), re.IGNORECASE)

# typical "I need help" phrasing -> usually not a publishable article
_HELP_RE = re.compile(
    r"\b(can someone help me|how do i fix|i am stuck|i'm stuck|"
    r"anyone know how|please help me|urgent help)\b",
    re.IGNORECASE,
)


def code_chars(text):
    """Total characters inside fenced + inline code blocks."""
    total = 0
    for m in _FENCE_RE.finditer(text):
        total += len(m.group(1))
    for m in _INLINE_CODE_RE.finditer(text):
        total += len(m.group(0))
    return total


def link_ratio(text):
    links = len(_LINK_RE.findall(text))
    words = len(_WORD_RE.findall(text))
    if words == 0:
        return 1.0
    return links / words


def unique_word_ratio(text):
    words = _WORD_RE.findall(text.lower())
    if len(words) < 20:
        # too few tokens for a stable signal; don't kill short-but-valid notes
        return 1.0
    return len(set(words)) / len(words)


def strip_code(text):
    """Return text with code blocks removed (for language detection)."""
    t = _FENCE_RE.sub(" ", text)
    t = _INLINE_CODE_RE.sub(" ", t)
    return t


def assess(text):
    """Return (keep: bool, reason: str, meta: dict)."""
    if not text:
        return False, "empty", {}
    n = len(text)
    cc = code_chars(text)
    meta = {"len": n, "code_chars": cc, "link_ratio": round(link_ratio(text), 3),
            "unique_ratio": round(unique_word_ratio(text), 3)}

    # 1. spam / promotional
    if _SPAM_RE.search(text):
        return False, "spam", meta
    # 2. pure link dump (e.g. link roundups with no prose)
    if link_ratio(text) > MAX_LINK_RATIO:
        return False, "link_dump", meta
    # 3. low lexical diversity (boilerplate / repeated fragments)
    if unique_word_ratio(text) < MIN_UNIQUE_RATIO:
        return False, "low_diversity", meta
    # 4. code-rich carve-out: even a short post with a real code block is kept
    if cc >= MIN_CODE_CHARS:
        return True, "code_rich", meta
    # 5. too thin even for prose
    if n < MIN_CONTENT_LEN:
        return False, "too_short", meta
    # 6. prose without enough words
    words = len(_WORD_RE.findall(text))
    if words < MIN_WORDS_NO_CODE:
        return False, "thin_prose", meta
    # 7. help-request without substance
    if _HELP_RE.search(text) and words < MIN_WORDS_NO_CODE * 2:
        return False, "help_request", meta
    return True, "ok", meta
