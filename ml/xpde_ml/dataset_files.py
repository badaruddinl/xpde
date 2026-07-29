from __future__ import annotations

from pathlib import Path


def dataset_manifest_path(dataset_path: Path) -> Path:
    """Keep one manifest name for plain and gzip-compressed CSV datasets."""

    base = (
        dataset_path.with_suffix("")
        if dataset_path.suffix.lower() == ".gz"
        else dataset_path
    )
    return base.with_suffix(".manifest.json")
