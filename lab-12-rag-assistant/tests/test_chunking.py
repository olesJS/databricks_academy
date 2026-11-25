"""Unit tests for rag/chunking.py. Pure Python: no Spark, Databricks or network."""

import pytest

from rag.chunking import (
    INTRO_SECTION,
    chunk_document,
    chunk_section,
    clean_text,
    count_tokens,
    split_sections,
)


# --------------------------------------------------------------------------- count_tokens


@pytest.mark.parametrize(
    "text, expected",
    [
        ("", 0),
        ("   \n\t ", 0),
        ("one", 1),            # round(1 / 0.75) = round(1.33)
        ("one two three", 4),  # round(3 / 0.75)
        ("one\ntwo\t\tthree   four", 5),  # any whitespace separates words; round(5.33)
    ],
)
def test_count_tokens(text, expected):
    assert count_tokens(text) == expected


# --------------------------------------------------------------------------- clean_text


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("a\r\nb", "a\nb"),
        ("a b", "a b"),
        ("a  \t  b", "a b"),
        ("a   \n   b", "a\nb"),
        ("a\n\n\n\n\nb", "a\n\nb"),
        ("a\n\nb", "a\n\nb"),
        ("  \n hello \n  ", "hello"),
    ],
)
def test_clean_text(raw, expected):
    assert clean_text(raw) == expected


# --------------------------------------------------------------------------- split_sections


def test_split_sections_without_headings_is_introduction():
    assert split_sections("Just some text.") == [(INTRO_SECTION, "Just some text.")]


def test_split_sections_builds_heading_paths():
    text = (
        "Intro text.\n"
        "== Career ==\n"
        "Career text.\n"
        "=== 2021 season ===\n"
        "Season text.\n"
        "== Legacy ==\n"
        "Legacy text."
    )
    assert split_sections(text) == [
        ("Introduction", "Intro text."),
        ("Career", "Career text."),
        ("Career > 2021 season", "Season text."),
        ("Legacy", "Legacy text."),  # level-2 heading pops the level-3 one
    ]


def test_split_sections_strips_heading_whitespace():
    assert split_sections("==   Career   ==\nText.") == [("Career", "Text.")]


def test_split_sections_drops_empty_bodies():
    text = "\n== Career ==\n=== 2021 ===\nText.\n== Empty ==\n   \n"
    assert split_sections(text) == [("Career > 2021", "Text.")]


def test_split_sections_skips_reference_sections_with_subsections():
    text = (
        "== Career ==\n"
        "A.\n"
        "== References ==\n"
        "Ref.\n"
        "=== Books ===\n"
        "Book.\n"
        "== Legacy ==\n"
        "L."
    )
    assert split_sections(text) == [("Career", "A."), ("Legacy", "L.")]


def test_split_sections_skip_is_case_insensitive_and_works_at_end():
    text = "== Career ==\nA.\n== External Links ==\nhttp://example.com"
    assert split_sections(text) == [("Career", "A.")]


def test_split_sections_single_equals_is_not_a_heading():
    assert split_sections("= Title =\nText.") == [(INTRO_SECTION, "= Title =\nText.")]


# --------------------------------------------------------------------------- chunk_section

# Each sentence has 3 words, so count_tokens == 4.
S = [f"Sentence {name} here." for name in ("one", "two", "three", "four", "five", "six")]


def test_chunk_section_empty_body():
    assert chunk_section("", max_tokens=100, overlap_tokens=10) == []


def test_chunk_section_short_body_is_one_chunk():
    body = "First line here.\nSecond line here."
    assert chunk_section(body, max_tokens=100, overlap_tokens=10) == [
        "First line here. Second line here."
    ]


def test_chunk_section_carries_overlap_between_chunks():
    body = " ".join(S)
    chunks = chunk_section(body, max_tokens=12, overlap_tokens=4)
    assert chunks == [
        " ".join(S[0:3]),
        " ".join(S[2:5]),  # starts with the last sentence of the previous chunk
        " ".join(S[4:6]),
    ]


def test_chunk_section_without_overlap_has_no_repeats():
    body = " ".join(S)
    chunks = chunk_section(body, max_tokens=12, overlap_tokens=0)
    assert chunks == [" ".join(S[0:3]), " ".join(S[3:6])]
    assert " ".join(chunks) == body


def test_chunk_section_splits_long_sentence_by_words():
    # max_tokens=8 -> at most int(8 * 0.75) = 6 words per unit
    words = [f"w{i}" for i in range(14)]
    chunks = chunk_section(" ".join(words), max_tokens=8, overlap_tokens=0)
    assert chunks == [" ".join(words[0:6]), " ".join(words[6:12]), " ".join(words[12:14])]


@pytest.mark.parametrize(
    "body, max_tokens, overlap_tokens",
    [
        (" ".join(S), 12, 4),
        # a short sentence (4 tokens) followed by a sentence of exactly max_tokens (8 tokens)
        ("Short one here. Exactly six words in this sentence.", 8, 4),
    ],
)
def test_chunk_section_respects_max_tokens(body, max_tokens, overlap_tokens):
    for chunk in chunk_section(body, max_tokens, overlap_tokens):
        assert count_tokens(chunk) <= max_tokens, chunk


# --------------------------------------------------------------------------- chunk_document

PAGE = {
    "page_id": 42,
    "title": "Monaco Grand Prix",
    "category": "circuit",
    "url": "https://en.wikipedia.org/wiki/Monaco_Grand_Prix",
    "revision_id": 123456,
    "text": (
        "The race is held in Monaco.\r\n"
        "== History ==\r\n"
        "It was first run in 1929.\r\n"
        "== References ==\r\n"
        "Some reference."
    ),
}

EXPECTED_KEYS = {
    "chunk_id", "chunk_size", "page_id", "title", "category", "section",
    "chunk_index", "chunk_text", "token_count", "url", "revision_id",
}


def test_chunk_document_rows():
    rows = chunk_document(PAGE, max_tokens=400, overlap_tokens=60)

    assert [r["section"] for r in rows] == ["Introduction", "History"]  # References skipped
    assert [r["chunk_index"] for r in rows] == [0, 1]                   # continues across sections
    assert [r["chunk_id"] for r in rows] == ["42_400_0000", "42_400_0001"]
    assert rows[0]["chunk_text"] == "Monaco Grand Prix > Introduction: The race is held in Monaco."
    assert rows[1]["chunk_text"] == "Monaco Grand Prix > History: It was first run in 1929."

    for row in rows:
        assert set(row) == EXPECTED_KEYS
        assert row["chunk_size"] == 400
        assert row["token_count"] == count_tokens(row["chunk_text"])
        assert "\r" not in row["chunk_text"]
        for key in ("page_id", "title", "category", "url", "revision_id"):
            assert row[key] == PAGE[key]


def test_chunk_document_chunk_index_spans_multiple_chunks_per_section():
    page = {**PAGE, "text": " ".join(S) + "\n== History ==\nIt was first run in 1929."}
    rows = chunk_document(page, max_tokens=12, overlap_tokens=0)
    assert [(r["section"], r["chunk_index"]) for r in rows] == [
        ("Introduction", 0),
        ("Introduction", 1),
        ("History", 2),
    ]
    assert rows[-1]["chunk_id"] == "42_12_0002"


def test_chunk_document_empty_text():
    assert chunk_document({**PAGE, "text": "  \n "}, max_tokens=400, overlap_tokens=60) == []
