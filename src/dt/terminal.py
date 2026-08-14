"""Streaming terminal-safety filter for job-controlled log output."""

from __future__ import annotations

import sys


class TerminalSanitizer:
    """Remove terminal control protocols without buffering an unbounded stream."""

    __slots__ = ("_omitted_kind", "_omitted_count", "_sequence", "_sequence_count")

    def __init__(self) -> None:
        self._omitted_kind: str | None = None
        self._omitted_count = 0
        self._sequence: str | None = None
        self._sequence_count = 0

    def _omit(self, kind: str, count: int = 1) -> str:
        if self._omitted_kind == kind:
            self._omitted_count += count
            return ""
        prefix = self._flush_omitted()
        self._omitted_kind = kind
        self._omitted_count = count
        return prefix

    def _flush_omitted(self) -> str:
        if self._omitted_kind is None:
            return ""
        count = self._omitted_count
        unit = "byte" if count == 1 else "bytes"
        kind = self._omitted_kind
        self._omitted_kind = None
        self._omitted_count = 0
        return f"[dt: omitted {count} {kind} {unit}]"

    def _finish_sequence(self) -> str:
        count = self._sequence_count
        self._sequence = None
        self._sequence_count = 0
        return self._omit("terminal-control", count)

    @staticmethod
    def _byte_count(char: str) -> int:
        return len(char.encode("utf-8", errors="surrogateescape"))

    def feed(self, text: str, *, final: bool = False) -> str:
        """Consume one decoded chunk and return only printable terminal text."""
        output: list[str] = []
        for char in text:
            code = ord(char)
            state = self._sequence
            if state is None:
                if char == "\x1b":
                    output.append(self._flush_omitted())
                    self._sequence = "esc"
                    self._sequence_count = self._byte_count(char)
                elif char == "\x9b":
                    output.append(self._flush_omitted())
                    self._sequence = "csi"
                    self._sequence_count = self._byte_count(char)
                elif char == "\x9d":
                    output.append(self._flush_omitted())
                    self._sequence = "osc"
                    self._sequence_count = self._byte_count(char)
                elif char == "\x90":
                    output.append(self._flush_omitted())
                    self._sequence = "dcs"
                    self._sequence_count = self._byte_count(char)
                elif char in {"\x98", "\x9e", "\x9f"}:
                    output.append(self._flush_omitted())
                    self._sequence = "string"
                    self._sequence_count = self._byte_count(char)
                elif char == "\x00":
                    output.append(self._omit("NUL", self._byte_count(char)))
                elif (code < 0x20 and char not in "\t\n") or 0x7F <= code <= 0x9F:
                    output.append(
                        self._omit("terminal-control", self._byte_count(char))
                    )
                else:
                    output.append(self._flush_omitted())
                    output.append(char)
                continue

            self._sequence_count += self._byte_count(char)
            if state == "esc":
                if char == "[":
                    self._sequence = "csi"
                elif char == "]":
                    self._sequence = "osc"
                elif char == "P":
                    self._sequence = "dcs"
                elif char in {"X", "^", "_"}:
                    # SOS, PM, and APC are string controls terminated by ST.
                    self._sequence = "string"
                else:
                    output.append(self._finish_sequence())
            elif state == "csi":
                if "@" <= char <= "~":
                    output.append(self._finish_sequence())
            elif state in {"osc", "dcs", "string"}:
                if state == "osc" and char == "\x07":
                    output.append(self._finish_sequence())
                elif char == "\x9c":
                    output.append(self._finish_sequence())
                elif char == "\x1b":
                    self._sequence = f"{state}_esc"
            elif state in {"osc_esc", "dcs_esc", "string_esc"}:
                if char == "\\":
                    output.append(self._finish_sequence())
                elif char != "\x1b":
                    self._sequence = state.removesuffix("_esc")

        if final:
            if self._sequence is not None:
                output.append(self._finish_sequence())
            output.append(self._flush_omitted())
        return "".join(output)


def sanitize_terminal_text(text: str) -> str:
    """Sanitize one complete bounded string."""
    return TerminalSanitizer().feed(text, final=True)


def main() -> int:
    """Filter stdin incrementally for ``dt logs --follow``."""
    sanitizer = TerminalSanitizer()
    while chunk := sys.stdin.read(64 * 1024):
        sys.stdout.write(sanitizer.feed(chunk))
        sys.stdout.flush()
    sys.stdout.write(sanitizer.feed("", final=True))
    sys.stdout.flush()
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through subprocess
    raise SystemExit(main())
