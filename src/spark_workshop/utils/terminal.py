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


def _center(value: str, width: int) -> str:
    trimmed = value.strip()
    if len(trimmed) > width:
        trimmed = trimmed[: width - 1] + "…"
    return trimmed.center(width)
