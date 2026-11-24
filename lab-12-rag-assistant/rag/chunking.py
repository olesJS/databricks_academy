import re

SKIP_SECTIONS = {
    "references", "external links", "see also", "notes", "further reading",
    "bibliography", "sources", "citations", "footnotes", "notes and references",
}

INTRO_SECTION = "Introduction"
HEADING_RE = re.compile(r"^(={2,6})\s*(.+?)\s*\1\s*$", re.MULTILINE)
SENTENCE_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"'(])")

# English text is approximately 0.75 words per token
WORDS_PER_TOKEN = 0.75


def count_tokens(text: str) -> int:
    return round(len(text.split()) / WORDS_PER_TOKEN)


def clean_text(text: str) -> str:
    text = text.replace("\r\n", "\n")       # Windows line breaks -> "\n"
    text = text.replace("\u00a0", " ")      # non-breaking space -> regular space
    text = re.sub(r"[ \t]+", " ", text)     # collapse runs of spaces/tabs into one space
    text = re.sub(r" *\n *", "\n", text)    # trim spaces around line breaks
    text = re.sub(r"\n{3,}", "\n\n", text)  # max one empty line between paragraphs
    return text.strip()


def split_sections(text: str) -> list[tuple[str, str]]:
    """
    Split an article into (section_path, body) pairs.
    section_path keeps the heading hierarchy, e.g. "Formula One career > 2021 season".
    Text before the first heading becomes "Introduction".
    Skipped sections (References, External links, ...) are dropped with their subsections.
    Sections with an empty body are dropped.
    """
    sections = []
    stack: list[tuple[int, str]] = []   # (level, heading)
    skip_level = None
    current_path = INTRO_SECTION
    pos = 0

    def add(path: str, body: str) -> None:
        body = body.strip()
        if body:
            sections.append((path, body))

    for match in HEADING_RE.finditer(text):
        if skip_level is None:
            add(current_path, text[pos:match.start()])

        level, heading = len(match.group(1)), match.group(2).strip()
        pos = match.end()

        if skip_level is not None and level > skip_level:
            continue  # still inside a skipped section
        skip_level = None

        stack = [(lvl, h) for lvl, h in stack if lvl < level]
        stack.append((level, heading))
        current_path = " > ".join(h for _, h in stack)

        if heading.lower() in SKIP_SECTIONS:
            skip_level = level

    if skip_level is None:
        add(current_path, text[pos:])
    return sections


def _split_units(body: str, max_tokens: int) -> list[str]:
    units = []
    max_words = int(max_tokens * WORDS_PER_TOKEN)
    for paragraph in body.split("\n"):
        for sentence in SENTENCE_RE.split(paragraph.strip()):
            if not sentence:
                continue
            words = sentence.split()
            # a single very long sentence is split by words
            for i in range(0, len(words), max_words):
                units.append(" ".join(words[i:i + max_words]))
    return units


def chunk_section(body: str, max_tokens: int, overlap_tokens: int) -> list[str]:
    chunks, current, current_tokens = [], [], 0

    for unit in _split_units(body, max_tokens):
        unit_tokens = count_tokens(unit)
        if current and current_tokens + unit_tokens > max_tokens:
            chunks.append(" ".join(current))
            # carry over the tail of the previous chunk as overlap
            overlap, overlap_size = [], 0
            for prev in reversed(current):
                size = count_tokens(prev)
                if overlap_size + size > overlap_tokens:
                    break
                overlap.insert(0, prev)
                overlap_size += size
            current, current_tokens = overlap, overlap_size
        current.append(unit)
        current_tokens += unit_tokens

    if current:
        chunks.append(" ".join(current))
    return chunks



def chunk_document(page: dict, max_tokens: int, overlap_tokens: int) -> list[dict]:
    """Turn one bronze page into chunk rows for silver."""
    rows = []
    chunk_index = 0
    for section, body in split_sections(clean_text(page["text"])):
        for text in chunk_section(body, max_tokens, overlap_tokens):
            chunk_text = f"{page['title']} > {section}: {text}"
            rows.append({
                "chunk_id": f"{page['page_id']}_{max_tokens}_{chunk_index:04d}",
                "chunk_size": max_tokens,
                "page_id": page["page_id"],
                "title": page["title"],
                "category": page["category"],
                "section": section,
                "chunk_index": chunk_index,
                "chunk_text": chunk_text,
                "token_count": count_tokens(chunk_text),
                "url": page["url"],
                "revision_id": page["revision_id"],
            })
            chunk_index += 1
    return rows
