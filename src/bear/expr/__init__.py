import polars as pl
from typing import Final

from bear.expr._correspondence import spatial_correspondence


NULL: Final = pl.lit(None)
EMPTY_STR: Final = pl.lit("")
NULL_UUID: Final = pl.lit("{00000000-0000-0000-0000-000000000000}")


def null_if_empty_str(string: pl.Expr) -> pl.Expr:
    return pl.when(string.eq(EMPTY_STR)).then(NULL).otherwise(string)


def normalize_str(string: pl.Expr) -> pl.Expr:
    return string.str.strip_chars().str.to_lowercase()


__all__ = [
    "NULL",
    "EMPTY_STR",
    "NULL_UUID",
    "null_if_empty_str",
    "normalize_str",
    "spatial_correspondence",
]
