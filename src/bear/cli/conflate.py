import polars as pl

from prefect import flow, task

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple, TypeVar

from bear._plugins import centroid_x, centroid_y
from bear.core.fips import FIPS, USCounty
from bear.expr._correspondence import (
    merge_footprints_and_addresses,
    spatial_correspondence,
)
from bear.providers import ProviderKind


@dataclass(slots=True)
class ConflateTaskOptions:
    county: USCounty
    output_directory: Path
    input_directory: Path

    def input(self) -> Path:
        return self.input_directory / f"conform/fips={self.county.fips}"


@dataclass(slots=True)
class ConflateProvider:
    kind: ProviderKind
    height: int

    @property
    def available(self) -> bool:
        return self.height > 0

    def query(self, scan: pl.LazyFrame) -> pl.LazyFrame:
        return scan.filter(pl.col("provider") == self.kind)


T = TypeVar("T")
ConflateTaskResult = Tuple[ConflateTaskOptions, T]


@task(name="Conflate - Perform spatial correspondence")
def perform_correspondence(
    a: pl.DataFrame, b: pl.DataFrame, use_distance: bool = False
) -> pl.DataFrame:
    return spatial_correspondence(
        a.lazy(), b.lazy(), use_distance=use_distance
    ).collect(streaming=True)


@task(name="Conflate - Merge Footprints and Addresses")
def perform_merge(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
    return merge_footprints_and_addresses(a.lazy(), b.lazy()).collect(
        streaming=True
    )


@task(name="Conflate - Write Entities to Disk")
def write_entities(opts: ConflateTaskOptions, conflated: pl.DataFrame) -> None:
    output = (
        opts.output_directory
        / f"conflate/entities/fips={opts.county.fips}/data.parquet"
    )

    output.parent.mkdir(parents=True, exist_ok=True)

    (
        conflated.lazy()
        .select(
            "id",
            "classification",
            "address",
            "height",
            "levels",
            pl.col("geometry").pipe(centroid_x).alias("x"),
            pl.col("geometry").pipe(centroid_y).alias("y"),
        )
        .collect(streaming=True)
        .write_parquet(output)
    )


@task(name="Conflate - Write Crossref to Disk")
def write_crossref(opts: ConflateTaskOptions, conflated: pl.DataFrame) -> None:
    output = (
        opts.output_directory
        / f"conflate/crossref/fips={opts.county.fips}/data.parquet"
    )

    output.parent.mkdir(parents=True, exist_ok=True)

    (
        conflated.lazy()
        .select(entity_id=pl.col("id"), footprint=pl.col("foreign"))
        .explode("footprint")
        .unnest("footprint")
        .rename({"key": "provider_id"})
        .with_columns(provider=pl.col("provider").cast(pl.Enum(ProviderKind)))
        .sort("entity_id", "provider", nulls_last=True)
        .collect(streaming=True)
        .write_parquet(output)
    )


@task(name="Conflate - Write Footprints to Disk")
def write_footprints(
    opts: ConflateTaskOptions, footprints: pl.DataFrame
) -> None:
    output = (
        opts.output_directory
        / f"conflate/footprints/fips={opts.county.fips}/data.parquet"
    )

    output.parent.mkdir(parents=True, exist_ok=True)

    (
        footprints.lazy()
        .select("provider", "id", "geometry")
        .collect(streaming=True)
        .write_parquet(output)
    )


@task(name="Conflate - Perform Conflation")
def conflate(opts: ConflateTaskOptions):
    # TODO(justin): organize this function

    input_path = opts.input()
    assert input_path.exists()
    assert input_path.is_dir()

    # Scan Providers
    # -------------------------------------------------------------------------
    scan = pl.scan_parquet(input_path)

    providers = {}
    for kind in ProviderKind.list_providers():
        providers[kind] = ConflateProvider(
            kind,
            scan.filter(pl.col("provider") == kind)
            .select(pl.len())
            .collect(streaming=True)
            .item(),
        )

        if not providers[kind].available:
            del providers[kind]

    # Conflate Footprints
    # -------------------------------------------------------------------------
    footprints: pl.DataFrame = perform_correspondence(
        providers[ProviderKind.openstreetmap]
        .query(scan)
        .collect(streaming=True),
        providers[ProviderKind.microsoft].query(scan).collect(streaming=True),
    )

    footprints = perform_correspondence(
        footprints,
        providers[ProviderKind.microsoft].query(scan).collect(streaming=True),
    )

    # Conflate Addresses
    # -------------------------------------------------------------------------
    if (
        ProviderKind.openaddresses in providers
        and ProviderKind.nad in providers
    ):
        addresses = perform_correspondence(
            providers[ProviderKind.nad].query(scan).collect(streaming=True),
            providers[ProviderKind.openaddresses]
            .query(scan)
            .collect(streaming=True),
            use_distance=True,
        )
    elif ProviderKind.nad in providers:
        addresses = (
            providers[ProviderKind.nad].query(scan).collect(streaming=True)
        )
    else:
        addresses = (
            providers[ProviderKind.openaddresses]
            .query(scan)
            .collect(streaming=True)
        )

    # Conflate footprints and addresses
    # -------------------------------------------------------------------------
    conflated: pl.DataFrame = perform_merge(footprints, addresses)

    # Write out entities data
    # -------------------------------------------------------------------------
    write_entities(opts, conflated)

    # Write out crossref data
    # -------------------------------------------------------------------------
    write_crossref(opts, conflated)

    # Write out footprints data
    # -------------------------------------------------------------------------
    write_footprints(opts, footprints)


@flow(name="BEAR Conflate Flow")
def conflate_workflow(fips: str, output_directory: Path, input_directory: Path):
    county = FIPS.county(fips)
    conflate.submit(
        ConflateTaskOptions(county, output_directory, input_directory)
    ).wait()
