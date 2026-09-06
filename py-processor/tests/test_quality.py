"""Quality gate unit tests. Cases absorbed from the old verify_tmp.py script."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quality import assess, strip_code, code_chars


CODE_POST = (
    "```python\n"
    "import os\n"
    "from pathlib import Path\n\n"
    "def walk_and_collect(root: Path, ext: str = '.md'):\n"
    "    files = []\n"
    "    for p in root.rglob('*' + ext):\n"
    "        if p.is_file():\n"
    "            files.append(p)\n"
    "    return files\n\n"
    "if __name__ == '__main__':\n"
    "    for f in walk_and_collect(Path('docs')):\n"
    "        print(f)\n"
    "```\n\n"
    "Docs: https://a.com/1 https://b.com/2 https://c.com/3 https://d.com/4 "
    "https://e.com/5 https://f.com/6 https://g.com/7 https://h.com/8"
)

PLAIN = " ".join([
    "The article explains how distributed systems handle consensus.",
    "PostgreSQL uses MVCC to give readers snapshot isolation.",
    "Redis streams are ideal for building durable ingestion pipelines.",
    "Python asyncio makes concurrent IO simple and readable.",
    "Simhash lets us find near duplicates at scale quite quickly.",
    "Trafilatura extracts clean markdown from messy web pages.",
    "Rate limiting protects upstream APIs from bursts of traffic.",
    "A dead letter queue keeps poison messages out of the way.",
])


def test_code_rich_post_with_many_links_is_kept():
    # P1 regression: a code-rich post used to be killed by the link_dump gate
    keep, reason, meta = assess(CODE_POST)
    assert keep
    assert reason == "code_rich"
    assert meta["quality_score"] > 0


def test_plain_article_ok_with_score():
    keep, reason, meta = assess(PLAIN)
    assert keep
    assert reason == "ok"
    assert meta["quality_score"] > 0


def test_link_dump_rejected():
    text = " ".join(f"https://site{i}.com/{i}" for i in range(40))
    keep, reason, _ = assess(text)
    assert not keep
    assert reason == "link_dump"


def test_short_note_with_link_gate_behavior():
    # words < 20: link_ratio falls back to 0 (not killed by link gate), but a
    # 52-char note is still below MIN_CONTENT_LEN (100) -> too_short. Documents
    # actual gate precedence; the old ad-hoc script only printed this case.
    keep, reason, _ = assess("Just tried this tip, works great: https://x.dev/abc")
    assert not keep
    assert reason == "too_short"


def test_empty_rejected():
    keep, reason, _ = assess("")
    assert not keep
    assert reason == "empty"


def test_spam_rejected():
    keep, reason, _ = assess(PLAIN + " Buy now and use this discount code today!")
    assert not keep
    assert reason == "spam"


def test_low_diversity_rejected():
    keep, reason, _ = assess("the same line repeated again " * 30)
    assert not keep
    assert reason == "low_diversity"


def test_help_request_rejected():
    keep, reason, _ = assess("Can someone help me fix this error? I am stuck here. " * 2)
    assert not keep
    assert reason in {"help_request", "thin_prose", "low_diversity"}


def test_score_clamped_to_unit_interval():
    for text in ("", PLAIN, CODE_POST, "a" * 5000):
        _, _, meta = assess(text)
        if meta:
            assert 0.0 <= meta["quality_score"] <= 1.0


def test_strip_code_and_code_chars():
    text = "prose here\n```py\nx = 1\n```\n`inline` tail"
    stripped = strip_code(text)
    assert "x = 1" not in stripped
    assert "inline" not in stripped
    assert code_chars(text) == len("x = 1\n") + len("`inline`")
