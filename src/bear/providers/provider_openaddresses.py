from typing import Optional

import polars as pl
import pyarrow as pa

from bear import expr
from bear.core.fips import USCounty
from bear.typing import ArrowBatchGenerator, Provider
from bear.providers.registry import register_provider


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
        return lf.select(
            id=pl.col("hash"),
            classification=expr.NULL,
            address=pl.concat_str(
                pl.col("number"),
                pl.col("street"),
                pl.col("unit"),
                separator=" ",
                ignore_nulls=True,
            ).pipe(expr.normalize_str),
            height=expr.NULL,
            levels=expr.NULL,
            geometry=pl.col("geometry"),
        )
