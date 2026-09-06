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
    if words < 20:
        # too few prose tokens for a meaningful link-density signal; don't kill
        # short notes or code-heavy snippets that carry references.
        return 0.0
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


def _quality_score(n, cc, lr, ur):
    """0-1 composite signal (informational): length + code richness + lexical
    diversity, minus a link-dump penalty. Clamped to [0, 1]. The keep/reject
    decision is made by the gates below; this score only feeds the
    `quality_score` column."""
    length_s = min(n / 3000.0, 1.0)
    code_s = min(cc / 800.0, 1.0)
    diversity_s = max(0.0, min(ur, 1.0))
    link_pen = min(lr / 0.3, 1.0)
    score = 0.4 * length_s + 0.3 * code_s + 0.3 * diversity_s - 0.5 * link_pen
    return max(0.0, min(score, 1.0))


def assess(text):
    """Return (keep: bool, reason: str, meta: dict)."""
    if not text:
        return False, "empty", {}
    n = len(text)
    cc = code_chars(text)
    lr = link_ratio(text)
    ur = unique_word_ratio(text)
    meta = {
        "len": n,
        "code_chars": cc,
        "link_ratio": round(lr, 3),
        "unique_ratio": round(ur, 3),
        "quality_score": round(_quality_score(n, cc, lr, ur), 3),
    }

    # 1. spam / promotional
    if _SPAM_RE.search(text):
        return False, "spam", meta
    # 2. code-rich carve-out runs BEFORE link-density / diversity checks:
    #    code+link posts (API docs, README-style, tutorials with references)
    #    would otherwise be miskilled by the link_dump gate below.
    if cc >= MIN_CODE_CHARS:
        return True, "code_rich", meta
    # 3. pure link dump (e.g. link roundups with no prose)
    if lr > MAX_LINK_RATIO:
        return False, "link_dump", meta
    # 4. low lexical diversity (boilerplate / repeated fragments)
    if ur < MIN_UNIQUE_RATIO:
        return False, "low_diversity", meta
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
