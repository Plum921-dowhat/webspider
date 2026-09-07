"""SFT export unit tests (parse_qa + format builders), no DB needed."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from export_sft import parse_qa, to_alpaca, to_sharegpt


DOC = (
    "# How to retry HTTP calls?\n\n"
    "I am calling an API that rate limits aggressively.\n\n"
    "## Top answer\n\n"
    "Use exponential backoff with full jitter, and honor Retry-After."
)


def test_parse_qa_splits_title_question_answer():
    got = parse_qa(DOC, min_answer_chars=0)
    assert got == (
        "How to retry HTTP calls?",
        "I am calling an API that rate limits aggressively.",
        "Use exponential backoff with full jitter, and honor Retry-After.",
    )


def test_parse_qa_missing_answer_is_none():
    no_answer = "# Title\n\nOnly a question, no answer section."
    assert parse_qa(no_answer, min_answer_chars=0) is None


def test_parse_qa_answer_too_short_filtered():
    short = "# Title\n\nQuestion body.\n\n## Top answer\n\nok"
    assert parse_qa(short, min_answer_chars=10) is None
    assert parse_qa(short, min_answer_chars=0) is not None


def test_parse_qa_no_title_is_none():
    assert parse_qa("plain text without heading", min_answer_chars=0) is None


def test_format_builders():
    title = "Q?"
    q, a = "body of question", "body of answer"
    alpaca = to_alpaca(title, q, a, "https://x/1", ["go"])
    assert alpaca["instruction"] == title
    assert alpaca["input"] == q
    assert alpaca["output"] == a
    assert alpaca["url"] == "https://x/1"

    share = to_sharegpt(title, q, a, "https://x/1", [])
    assert share["conversations"][0]["from"] == "human"
    assert title in share["conversations"][0]["value"]
    assert share["conversations"][1]["from"] == "gpt"
    assert share["conversations"][1]["value"] == a
