from typing import Optional

import polars as pl
import pyarrow as pa

from bear import expr
from bear.core.fips import USCounty
from bear.typing import ArrowBatchGenerator, Provider
from bear.providers.registry import register_provider


@register_provider("nad")
class NADProvider(Provider):
    """`National Address Database (NAD) <https://www.transportation.gov/gis/national-address-database>`_ Provider

    This data is a work of the federal government and is not subject to copyright protection
    in accordance with 17 U.S.C. § 105. It is available for re-use without limitation or restriction.
    See the `NAD disclaimer <https://www.transportation.gov/mission/open/gis/national-address-database/national-address-database-nad-disclaimer>`_
    for more details.
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
        return lf.select(
            id=pl.coalesce(
                [
                    (
                        pl.when(pl.col("UUID").eq(expr.NULL_UUID))
                        .then(expr.NULL)
                        .otherwise(pl.col("UUID"))
                    ),
                    pl.col("NatGrid"),
                ]
            ),
            classification=(
                pl.when(pl.col("Addr_Type").is_in(["Unknown", "Other"]))
                .then(expr.NULL)
                .otherwise(pl.col("Addr_Type"))
            ),
            address=pl.concat_str(
                pl.col("AddNo_Full"),
                pl.col("StNam_Full"),
                pl.col("SubAddress"),
                separator=" ",
                ignore_nulls=True,
            ).pipe(expr.normalize_str),
            height=expr.NULL,
            levels=expr.NULL,
            geometry=pl.col("geometry"),
        )
