"""Terminal formatting helpers for workshop-friendly logs."""

DEFAULT_WIDTH = 88


def terminal_section(
    title: str,
    subtitle: str | None = None,
    *,
    width: int = DEFAULT_WIDTH,
) -> str:
    """Return a readable section divider for submit logs."""

    normalized_width = max(width, 40)
    content_width = normalized_width - 4
    lines = [_center(title.upper(), content_width)]
    if subtitle:
        lines.append(_center(subtitle, content_width))

    border = "═" * (normalized_width - 2)
    body = "\n".join(f"║ {line} ║" for line in lines)
    return f"\n╔{border}╗\n{body}\n╚{border}╝"


def terminal_box(lines: list[str], *, width: int = DEFAULT_WIDTH) -> str:
    """Return a multi-line terminal box for classroom-friendly summaries."""

    normalized_width = max(width, 40)
    content_width = normalized_width - 4
    border = "═" * (normalized_width - 2)
    rendered = [f"\n╔{border}╗"]
    for line in lines:
        if not line:
            rendered.append(f"║ {' ' * content_width} ║")
            continue
        for wrapped in _wrap_line(line, content_width):
            rendered.append(f"║ {wrapped.ljust(content_width)} ║")
    rendered.append(f"╚{border}╝")
    return "\n".join(rendered)


def _center(value: str, width: int) -> str:
    trimmed = value.strip()
    if len(trimmed) > width:
        trimmed = trimmed[: width - 1] + "…"
    return trimmed.center(width)


def _wrap_line(line: str, width: int) -> list[str]:
    words = line.split()
    if not words:
        return [""]
    wrapped: list[str] = []
    current = words[0]
    for word in words[1:]:
        if len(current) + 1 + len(word) > width:
            wrapped.append(current)
            current = word
        else:
            current = f"{current} {word}"
    wrapped.append(current)
    return wrapped
