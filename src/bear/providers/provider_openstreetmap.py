from typing import Optional

import polars as pl
import pyarrow as pa

from bear import expr
from bear.core.fips import USCounty
from bear.typing import ArrowBatchGenerator, Provider
from bear.providers.registry import register_provider


@register_provider("openstreetmap")
class OpenStreetMapProvider(Provider):
    """[OpenStreetMap](https://www.openstreetmap.org) Provider

    This data is licensed by the OpenStreetMap Foundation under the [Open Data Commons
    Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/).
    """

    @classmethod
    def epsg(cls) -> int:
        raise NotImplementedError()

    @classmethod
    def schema(cls) -> Optional[pa.Schema]:
        raise NotImplementedError()

    @classmethod
    def read(cls, county: USCounty, *args, **kwargs) -> ArrowBatchGenerator:
        raise NotImplementedError()

    @classmethod
    def conform(cls, lf: pl.LazyFrame, *args, **kwargs) -> pl.LazyFrame:
        FT_TO_M = 0.3048

        # Retrieve only features with building key
        lf = lf.filter(pl.col("building").is_not_null())

        # Initial conformance
        lf = lf.with_columns(
            id=pl.coalesce(["osm_id", "osm_way_id"]),
            classification=pl.coalesce(
                ["building", "amenity", "leisure"]
            ).replace("yes", None),
            address=pl.concat_str(
                ["name", "addr_housenumber", "addr_street", "addr_unit"],
                separator=" ",
                ignore_nulls=True,
            ),
            levels=pl.col("building_levels").str.replace_all(
                "`|''|\\+|(PK)|\\>|±", ""
            ),
        ).filter(
            pl.col("classification")
            .is_null()
            .or_(
                pl.col("classification")
                .is_in(["parking", "parking_space"])
                .not_()
            )
            .and_(
                pl.col("dataset")
                .is_null()
                .or_(pl.col("dataset") != "UniversityPly")
            )
        )

        # Handle height
        lf = lf.with_columns(
            height=pl.when(pl.col("height").is_in(["0", "0.0"]))
            .then(expr.NULL)
            .when(pl.col("height").str.contains(";", literal=True))
            .then(
                pl.col("height")
                .str.split(";")
                .list.eval(pl.element().cast(pl.Float64, strict=False))
                .list.max()
            )
            .when(pl.col("height").str.contains("ft", literal=True))
            .then(
                pl.col("height")
                .str.replace("[ft\\.]", "", literal=True)
                .str.strip_chars()
                .cast(pl.Float64, strict=False)
                .mul(FT_TO_M)
            )
            .when(pl.col("height").str.contains("m", literal=True))
            .then(
                pl.col("height")
                .str.replace("m", "", literal=True)
                .str.strip_chars()
                .cast(pl.Float64, strict=False)
            )
            .when(pl.col("height").str.ends_with("'"))
            .then(
                pl.col("height")
                .str.strip_suffix("'")
                .str.strip_chars()
                .cast(pl.Float64, strict=False)
                .mul(FT_TO_M)
            )
            .otherwise(
                pl.col("height")
                .str.strip_chars()
                .cast(pl.Float64, strict=False)
            )
        )

        # Handle levels
        lf = lf.with_columns(
            levels=pl.when(pl.col("levels").is_in(["0", "Default"]))
            .then(expr.NULL)
            .when(pl.col("levels").is_in(["Bi-Level", "Split"]))
            .then(2)
            .when(
                pl.col("levels").str.contains(",", literal=True)
                & pl.col("classification").eq("school")
            )
            .then(expr.NULL)
            .when(pl.col("levels").str.contains(".5", literal=True))
            .then(
                pl.col("levels")
                .str.replace("\\.5.*", "")
                .cast(pl.Int32, strict=False)
                .add(1)
            )
            .when(pl.col("levels").str.contains("1/2"))
            .then(
                pl.col("levels")
                .str.replace("1/2", "", literal=True)
                .str.strip_chars()
                .cast(pl.Int32, strict=False)
                .add(1)
            )
            .when(pl.col("levels").str.contains(",", literal=True))
            .then(pl.col("levels").str.split(",").list.len().cast(pl.Int32))
            .when(pl.col("levels").str.contains(";", literal=True))
            .then(
                pl.col("levels")
                .str.split(";")
                .list.eval(pl.element().cast(pl.Int32, strict=False))
                .list.max()
            )
            .when(pl.col("levels").str.contains("-", literal=True))
            .then(
                pl.col("levels")
                .str.split("-")
                .list.eval(pl.element().cast(pl.Int32, strict=False))
                .list.max()
            )
            .otherwise(pl.col("levels").cast(pl.Int32, strict=False)),
        )

        # Finalize
        return lf.with_columns(
            id=pl.col("id"),
            height=pl.when(pl.col("height") < 0)
            .then(expr.NULL)
            .otherwise(pl.col("height")),
            levels=pl.when(pl.col("levels") > 110)
            .then(expr.NULL)
            .otherwise(pl.col("levels")),
        ).select(
            [
                "id",
                "classification",
                "address",
                "height",
                "levels",
                "geometry",
            ]
        )
