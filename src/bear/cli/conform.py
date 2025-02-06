import polars as pl
import pyogrio

from prefect import flow, task
from prefect.futures import PrefectFuture

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, TypeVar

import bear.providers.provider_microsoft
import bear.providers.provider_nad
import bear.providers.provider_openaddresses
import bear.providers.provider_openstreetmap
import bear.providers.provider_usa_structures  # noqa: F401

from bear.core import schema
from bear.core.fips import FIPS, USCounty
from bear.providers.registry import ProviderRegistry
from bear.typing import Provider


@dataclass(slots=True)
class ConformTaskOptions:
    county: USCounty
    provider_name: str
    input_directory: Path = Path(".")
    output_directory: Path = Path(".")

    def provider(self) -> Provider:
        return ProviderRegistry.get(self.provider_name)

    def output(self) -> Path:
        return (
            self.output_directory
            / f"conform/fips={self.county.fips}/provider={self.provider_name}/data.parquet"
        )

    def input(self) -> Path:
        # TODO(justin): input paths are temporarily based on
        # a local, non-reproducible setup. This will change once
        # the extract workflow is built.
        return self.input_directory / f"{self.provider_name}.vrt"


T = TypeVar("T")
ConformTaskResult = Tuple[ConformTaskOptions, T]


@task(name="Conform - Load Raw Data from Disk")
def conform_load(
    opts: ConformTaskOptions,
) -> ConformTaskResult[Optional[pl.DataFrame]]:
    input_path = opts.input()

    # Workaround until
    # https://github.com/geopandas/pyogrio/issues/501
    # https://github.com/OSGeo/gdal/pull/11293
    # ----------------------------------------------------------------------
    tbl = pyogrio.read_dataframe(
        input_path, mask=opts.county.geometry, use_arrow=False
    )
    tbl = pl.from_pandas(tbl.to_wkb())
    # ----------------------------------------------------------------------
    # Original:
    # > meta, tbl = pyogrio.read_arrow(input_path, mask=spec.county.geometry)
    # > geometry_name = meta["geometry_name"] or "wkb_geometry"
    # > geometry_column = tbl.column(geometry_name)
    # > tbl = tbl.drop(geometry_name).append_column("geometry", geometry_column)
    # > tbl = pl.from_arrow(tbl)

    assert isinstance(tbl, pl.DataFrame)

    if tbl.height == 0:
        tbl = None

    return opts, tbl


@task(name="Conform - Perform Data Conformance")
def conform_process(
    opts: ConformTaskOptions, tbl: Optional[pl.DataFrame]
) -> ConformTaskResult[Optional[pl.DataFrame]]:
    if tbl is not None and tbl.height > 0:
        tbl = opts.provider().conform(tbl.lazy()).collect(streaming=True)

    return (opts, tbl)


@task(name="Conform - Write Processed Data to Disk")
def conform_save(opts: ConformTaskOptions, tbl: Optional[pl.DataFrame]) -> None:
    output_path = opts.output()
    if tbl is None or output_path.exists():
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    tbl.cast(schema.conform).write_parquet(output_path, compression="zstd")  # type: ignore


type FutureType = PrefectFuture[ConformTaskResult[Optional[pl.DataFrame]]]


@flow(name="BEAR Conform Flow")
def conform_workflow(
    fips: str,
    provider: str,
    output_directory: Path,
    input_directory: Path,
) -> None:
    county = FIPS.county(fips)

    if not input_directory.exists():
        raise FileNotFoundError(f"Path {input_directory} does not exist.")

    if not input_directory.is_dir():
        raise NotADirectoryError(f"Path {input_directory} is not a directory")

    output_directory.mkdir(parents=True, exist_ok=True)

    future_load = conform_load.submit(
        ConformTaskOptions(
            county,
            provider,
            input_directory,
            output_directory,
        )
    )

    future_process = conform_process.submit(*future_load.result())
    future_save = conform_save.submit(*future_process.result())
    future_save.wait()
