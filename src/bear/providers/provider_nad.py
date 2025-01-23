from typing import Optional

import polars as pl
import pyarrow as pa

from bear.core.fips import USCounty
from bear.typing import ArrowBatchGenerator, Provider
from bear.providers.registry import register_provider


@register_provider("nad")
class NADProvider(Provider):
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
        raise NotImplementedError()
