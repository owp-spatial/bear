import polars as pl

from typing import Final

conform: Final = pl.Schema(
    {
        "id": pl.String(),
        "classification": pl.String(),
        "address": pl.String(),
        "height": pl.Float64(),
        "levels": pl.Int32(),
        "geometry": pl.Binary(),
    }
)
