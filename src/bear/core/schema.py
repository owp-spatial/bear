import polars as pl

from typing import Final

conform: Final = pl.Schema(
    {
        # An identifier
        "id": pl.String(),
        # Building classification type
        "classification": pl.String(),
        # Street address
        "address": pl.String(),
        # Height of building, in meters
        "height": pl.Float64(),
        # Number of levels/stories within the building
        "levels": pl.Int32(),
        # Geometry of building (either POINT or POLYGON) stored as WKB
        "geometry": pl.Binary(),
    }
)
