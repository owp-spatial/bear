from pathlib import Path
from typing import Iterable
from dataclasses import dataclass

import polars as pl
import pyogrio
from prefect import flow, task
from prefect.futures import as_completed, wait
from prefect.task_runners import ThreadPoolTaskRunner

import bear.providers.provider_microsoft  # noqa
import bear.providers.provider_nad  # noqa
import bear.providers.provider_openaddresses  # noqa
import bear.providers.provider_openstreetmap  # noqa
import bear.providers.provider_usa_structures  # noqa

from bear.core import schema
from bear.core.fips import FIPS, USCounty
from bear.providers.registry import ProviderRegistry
from bear.typing import Provider

BEAR_DIR = Path(".bear")

DATA_DIR = Path("/mnt/nvme/footprint-coverage-data/raw")


@dataclass(slots=True)
class ConformTask:
    county: USCounty
    provider_name: str

    @property
    def provider(self) -> Provider:
        return ProviderRegistry.get(self.provider_name)

    def output(self) -> Path:
        data_file = f"conform/fips={self.county.fips}/provider={self.provider_name}/data.parquet"
        return BEAR_DIR / data_file

    def input(self) -> Path:
        if self.provider_name == "usa_structures":
            data_file = "fema.vrt"
        else:
            data_file = f"{self.provider_name}.vrt"

        return DATA_DIR / data_file


@task
def conform_load(spec: ConformTask) -> tuple[ConformTask, pl.DataFrame]:
    input_path = spec.input()

    # Workaround until
    # https://github.com/geopandas/pyogrio/issues/501
    # https://github.com/OSGeo/gdal/pull/11293
    # ----------------------------------------------------------------------
    tbl = pyogrio.read_dataframe(
        input_path, mask=spec.county.geometry, use_arrow=False
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

    return spec, spec.provider.conform(tbl.lazy()).collect()


@task
def conform_save(spec: ConformTask, tbl: pl.DataFrame) -> None:
    output_path = spec.output()
    if output_path.exists():
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tbl.cast(schema.conform).write_parquet(output_path, compression="zstd")  # type: ignore


@flow(task_runner=ThreadPoolTaskRunner(max_workers=8))  # type: ignore
def conform(fips: str, providers: Iterable[str]):
    county = FIPS.county(fips)

    load_futures = []
    for provider in providers:
        spec = ConformTask(county, provider)
        future = conform_load.submit(spec)
        load_futures.append(future)

    save_futures = []
    for future in as_completed(load_futures):
        new_future = conform_save.submit(*future.result())
        save_futures.append(new_future)

    wait(save_futures)


class ConflateTask:
    __slots__ = "_county"

    def __init__(self, fips: str):
        self._county = FIPS.county(fips)

    def county(self) -> USCounty:
        return self._county

    def output(self) -> Path:
        return BEAR_DIR / f"conflate/fips={self._county.fips}/data.parquet"

    def input(self) -> Path:
        return BEAR_DIR / f"conform/fips={self._county.fips}"


@flow
def conflate(fips: str):
    task = ConflateTask(fips)
    assert task.input().exists()
    assert task.input().is_dir()


if __name__ == "__main__":
    FIPS.initialize()
    providers = ProviderRegistry.providers()
    conform("37129", providers)
