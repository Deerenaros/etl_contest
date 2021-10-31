from .etl import Extractor, Loader


def process(src, dst) -> None:
    Loader(
        Extractor(src, "transactions"),
        dst,
        "transactions_denormalized"
    ).load()
