"""Chunker + export helper unit tests (no DB needed)."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from chunker import split_markdown, _parse_sections
from rag_export import load_state, save_state


SE_DOC = (
    "# How to retry HTTP calls?\n\n"
    "I am calling an API that rate limits aggressively.\n\n"
    "## Top answer\n\n"
    "Use exponential backoff with full jitter, and honor Retry-After.\n\n"
    "Libraries like tenacity implement this out of the box."
)


def test_parse_sections_paths():
    sections = _parse_sections(SE_DOC)
    assert sections[0][0] == ["How to retry HTTP calls?"]
    assert sections[1][0] == ["How to retry HTTP calls?", "Top answer"]
    assert "exponential backoff" in sections[1][1]


def test_small_doc_single_chunks():
    out = split_markdown(SE_DOC)
    assert len(out) == 2
    assert out[0]["heading_path"] == ["How to retry HTTP calls?"]
    assert out[1]["heading_path"][-1] == "Top answer"


def test_oversize_section_split_with_overlap():
    para = " ".join(["word"] * 60)  # ~300 chars
    body = "# Title\n\n" + "\n\n".join([para] * 12)  # ~3600 chars
    out = split_markdown(body, target=800, overlap=320)
    assert len(out) >= 4
    # all chunks under target + one paragraph of slack
    for c in out:
        assert len(c["text"]) <= 800 + 320
    # overlap: consecutive chunks share content
    assert out[1]["text"][:50] in out[0]["text"] + out[1]["text"]
    # heading glued to the first chunk
    assert out[0]["text"].startswith("# Title")


def test_oversize_single_paragraph_not_hard_split():
    code = "```python\n" + "\n".join(["x = 1"] * 400) + "\n```"
    out = split_markdown("# T\n\n" + code, target=200)
    assert len(out) == 1 and len(out[0]["text"]) > 200  # kept whole


def test_tiny_chunks_dropped():
    out = split_markdown("# T\n\nok\n\n## H2\n\n" + "x " * 100)
    assert all(len(c["text"]) >= 30 for c in out)


def test_state_roundtrip(tmp_path):
    p = str(tmp_path / "state.json")
    save_state(p, 42)
    assert load_state(p) == 42
    assert load_state(str(tmp_path / "missing.json")) == 0
