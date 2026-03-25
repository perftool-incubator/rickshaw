"""Custom exception hierarchy for source-images-service."""

from __future__ import annotations


class SourceImagesError(Exception):
    """Base exception for all source-images-service errors."""


class SubprocessError(SourceImagesError):
    """A subprocess exited with a non-zero return code or timed out."""

    def __init__(
        self,
        message: str,
        *,
        cmd: str = "",
        return_code: int = -1,
        output: str = "",
    ) -> None:
        super().__init__(message)
        self.cmd = cmd
        self.return_code = return_code
        self.output = output


class RegistryError(SourceImagesError):
    """A container registry operation failed (push, pull, inspect, delete)."""


class HashCalculationError(SourceImagesError):
    """Image tag hash calculation failed."""


class WorkshopBuildError(SourceImagesError):
    """Workshop build or output parsing failed."""
