from typing import Optional

import polars as pl
import polars_hash as plh
import pyarrow as pa

from bear import expr
from bear.core.fips import USCounty
from bear.typing import ArrowBatchGenerator, Provider
from bear.providers.registry import register_provider


@register_provider("microsoft")
class MicrosoftProvider(Provider):
    """[Microsoft Building Footprints](https://github.com/microsoft/GlobalMLBuildingFootprints) Provider

    This data is licensed by Microsoft under the [Open Data Commons
    Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/>).
    """

    @classmethod
    def epsg(cls) -> int:
        return 4326

    @classmethod
    def schema(cls) -> Optional[pa.Schema]:
        return pa.schema(
            {
                "height": pa.float64(),
                "confidence": pa.float64(),
                "geometry": pa.binary(),
            }
        )

    @classmethod
    def read(cls, county: USCounty, *args, **kwargs) -> ArrowBatchGenerator:
        raise NotImplementedError()

    @classmethod
    def conform(cls, lf: pl.LazyFrame, *args, **kwargs) -> pl.LazyFrame:
        return lf.select(
            id=plh.col("geometry").bin.encode("base64").chash.sha256(),  # type: ignore
            classification=expr.NULL,
            address=expr.NULL,
            height=(
                pl.when(pl.col("height") < 0)
                .then(expr.NULL)
                .otherwise(pl.col("height"))
            ),
            levels=expr.NULL,
            geometry=pl.col("geometry"),
        )
