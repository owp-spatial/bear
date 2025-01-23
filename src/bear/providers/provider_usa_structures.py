from typing import Optional

import polars as pl
import pyarrow as pa

from bear import expr
from bear.core.fips import USCounty
from bear.typing import ArrowBatchGenerator, Provider
from bear.providers.registry import register_provider


@register_provider("usa_structures")
class USAStructuresProvider(Provider):
    """[USA Structures](https://gis-fema.hub.arcgis.com/pages/usa-structures) Provider

    The data is licensed under the Creative Commons By Attribution (CC BY 4.0) license.
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
            id=pl.col("UUID"),
            classification=pl.col("OCC_CLS")
            .pipe(expr.normalize_str)
            .pipe(expr.null_if_empty_str),
            address=pl.col("PROP_ADDR").pipe(expr.normalize_str),
            height=pl.col("HEIGHT"),
            levels=expr.NULL,
            geometry=pl.col("geometry"),
        )
