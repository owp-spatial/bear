from typing import Optional

import polars as pl
import pyarrow as pa

from bear import expr
from bear.core.fips import USCounty
from bear.typing import ArrowBatchGenerator, Provider
from bear.providers.registry import register_provider
from bear._plugins import centroid_x, centroid_y, explode_multipoint


@register_provider("openaddresses")
class OpenAddressesProvider(Provider):
    """[OpenAddresses](https://openaddresses.io/) Provider

    The datasets provided by OpenAddresses are individually licensed.
    Most are available under open licenses, but there is no guarantee.
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
        lf = (
            lf.drop("id", "region")
            .unique()
            .with_columns(X=centroid_x("geometry"), Y=centroid_y("geometry"))
            .with_columns(
                count=pl.col("hash")
                .over(["X", "Y", "number", "street"])
                .count(),
                group=pl.struct("X", "Y").rank("dense"),
                address=pl.concat_str(
                    pl.col("number"),
                    pl.col("street"),
                    # pl.col("unit"),
                    separator=" ",
                    ignore_nulls=True,
                ).pipe(expr.normalize_str),
            )
            .filter(
                pl.col("address").is_not_null().and_(pl.col("address") != "0")
            )
        )

        singles = lf.filter(pl.col("count") == 1).with_columns(
            unit_count=1, key_id=pl.col("hash")
        )

        multis = (
            lf.filter(pl.col("count") > 1)
            .with_columns(
                pl.selectors.by_index(range(6)).backward_fill().over("group"),
                unit_count=pl.col("group").count().over("group"),
                key_id=pl.col("hash").first().over("group"),
            )
            .group_by("group")
            .first()
        )

        return (
            pl.concat([singles, multis], how="diagonal_relaxed")
            .drop("hash", "group", "count")
            .select(
                id=pl.col("key_id"),
                classification=expr.NULL,
                address=pl.col("address"),
                height=expr.NULL,
                levels=expr.NULL,
                geometry=explode_multipoint("geometry"),
            )
        )
