"""Export StackExchange Q&A pairs as SFT instruction data.

Each stackexchange article is composed as "# {title}\n\n{question}\n\n## Top
answer\n\n{answer}" — this script splits that structure back into
instruction/response pairs and writes alpaca or sharegpt JSONL.

Filters: minimum quality_score, minimum answer length, dedup by question
title. Usage:
    python export_sft.py --format alpaca --out sft_alpaca.jsonl
    python export_sft.py --format sharegpt --min-answer-chars 200
"""
import argparse
import json
import re

from store import get_conn, putconn

_TITLE_RE = re.compile(r"#\s+(.+)")  # first line only ('.' never matches \n)
_ANSWER_SEP = "\n## Top answer\n"


def parse_qa(content_md, min_answer_chars=0):
    """Split a composed Q&A document into (title, question_md, answer_md).
    Returns None when the structure or the answer length doesn't qualify."""
    if not content_md:
        return None
    m = _TITLE_RE.match(content_md.strip())
    if not m:
        return None
    title = m.group(1).strip()
    rest = content_md.strip()[m.end():].strip()
    if _ANSWER_SEP not in rest:
        return None
    question_md, _, answer_md = rest.partition(_ANSWER_SEP)
    question_md = question_md.strip()
    answer_md = answer_md.strip()
    if not question_md or len(answer_md) < min_answer_chars:
        return None
    return title, question_md, answer_md


def to_alpaca(title, question_md, answer_md, url, tags):
    return {
        "instruction": title,
        "input": question_md,
        "output": answer_md,
        "url": url,
        "tags": tags or [],
    }


def to_sharegpt(title, question_md, answer_md, url, tags):
    return {
        "conversations": [
            {"from": "human", "value": f"{title}\n\n{question_md}".strip()},
            {"from": "gpt", "value": answer_md},
        ],
        "url": url,
        "tags": tags or [],
    }


def export_sft(out_path, fmt="alpaca", min_quality=0.0, min_answer_chars=100):
    conn = get_conn()
    written = 0
    seen_titles = set()
    fmt = fmt.lower()
    if fmt not in ("alpaca", "sharegpt"):
        raise SystemExit(f"unsupported format: {fmt}")
    try:
        with conn.cursor(name="sft_cur") as cur:
            cur.itersize = 500
            cur.execute(
                """
                SELECT url, title, content_md, quality_score, tags
                FROM articles
                WHERE source_type = 'stackexchange' AND content_md IS NOT NULL
                ORDER BY quality_score DESC NULLS LAST, id
                """
            )
            with open(out_path, "w", encoding="utf-8") as f:
                for url, title, content_md, score, tags in cur:
                    if score is not None and score < min_quality:
                        continue
                    qa = parse_qa(content_md, min_answer_chars)
                    if qa is None:
                        continue
                    qa_title, question_md, answer_md = qa
                    key = qa_title.lower()
                    if key in seen_titles:  # same question re-answered: first (best) wins
                        continue
                    seen_titles.add(key)
                    if fmt == "alpaca":
                        rec = to_alpaca(qa_title, question_md, answer_md, url, tags)
                    else:
                        rec = to_sharegpt(qa_title, question_md, answer_md, url, tags)
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    written += 1
    finally:
        putconn(conn)
    print(f"exported {written} SFT samples ({fmt}) -> {out_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="sft_alpaca.jsonl")
    ap.add_argument("--format", default="alpaca", choices=["alpaca", "sharegpt"])
    ap.add_argument("--min-quality", type=float, default=0.0,
                    help="minimum article quality_score (0 = no filter)")
    ap.add_argument("--min-answer-chars", type=int, default=100,
                    help="drop pairs whose answer is shorter than this")
    args = ap.parse_args()
    export_sft(args.out, fmt=args.format,
               min_quality=args.min_quality, min_answer_chars=args.min_answer_chars)
