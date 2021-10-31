from typing import Any
from .etl import Extractor, Loader


def process(src: dict[str, Any], dst: dict[str, Any]) -> None:
    Loader(
        Extractor(src, "transactions"),
        dst,
        "transactions_denormalized"
    ).load()
