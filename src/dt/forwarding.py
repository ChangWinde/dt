"""Immutable laptop-to-head command construction."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Protocol


class ForwardCall(Protocol):
    def __call__(self, head: str, argv: list[str]) -> int: ...


@dataclass(frozen=True, slots=True)
class HeadCommand:
    """A resolved head and an immutable argv payload."""

    head: str
    parts: tuple[str, ...]

    @classmethod
    def start(cls, head: str, command: str, *arguments: object) -> "HeadCommand":
        return cls(head=head, parts=(command, *(str(value) for value in arguments)))

    def arguments(self, *values: object) -> "HeadCommand":
        return replace(
            self,
            parts=(*self.parts, *(str(value) for value in values)),
        )

    def option(self, flag: str, value: object | None) -> "HeadCommand":
        if value is None:
            return self
        return self.arguments(flag, value)

    def flag(self, flag: str, enabled: bool) -> "HeadCommand":
        return self.arguments(flag) if enabled else self

    def repeat(self, flag: str, values: Iterable[object]) -> "HeadCommand":
        command = self
        for value in values:
            command = command.option(flag, value)
        return command

    def passthrough(self, values: list[str]) -> "HeadCommand":
        return self.arguments("--", *values)

    def argv(self) -> list[str]:
        """Return a fresh list suitable for subprocess and legacy callbacks."""
        return list(self.parts)

    def invoke(self, call: ForwardCall) -> int:
        return call(self.head, self.argv())
