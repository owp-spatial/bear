import polars as pl
from polars._typing import IntoExpr

from scourgify import normalize_address_record

from dataclasses import dataclass
from typing import Iterable

import bear._plugins as udf


@dataclass(slots=True)
class JoinArgs:
    id: str = "id"
    geometry: str = "geometry"


def addr_normalize(s: str) -> str:
    try:
        result: dict[str, str] = dict(
            **normalize_address_record(s, long_hand=True)
        )
    except Exception:
        return ""

    if result["address_line_1"] is None:
        result["address_line_1"] = ""

    if result["address_line_2"] is None:
        result["address_line_2"] = ""

    if len(result) == 0:
        return ""
    else:
        return " ".join([result["address_line_1"], result["address_line_2"]])


def sc_initialize_lazy(
    lf: pl.LazyFrame,
    args: JoinArgs,
    suffix: str,
    *,
    row_index_name="index",
) -> pl.LazyFrame:
    return (
        lf.with_row_index(row_index_name)
        .with_columns(
            foreign=pl.coalesce(
                pl.col("^foreign$"),
                pl.lit(
                    [],
                    dtype=pl.List(
                        pl.Struct(
                            {
                                "provider": pl.String(),
                                "key": pl.String(),
                            }
                        )
                    ),
                ),
            ),
        )
        .rename({args.geometry: "geometry", args.id: "id"})
        .select(pl.all().name.suffix(suffix))
    )


def sc_coalesce_attr(attr: str) -> pl.Expr:
    return pl.coalesce(pl.selectors.starts_with(attr))


def sc_correspond_overlap(
    lf: pl.LazyFrame,
    left_col: IntoExpr,
    right_col: IntoExpr,
    *,
    column_name: str = "corresponds",
    threshold: IntoExpr = 0.3,
) -> pl.LazyFrame:
    return (
        lf.with_columns(
            # Intersection Area
            udf.area(udf.intersection(left_col, right_col)).alias(
                "area_intersection"
            ),
            # Relative Area
            pl.min_horizontal(udf.area(left_col), udf.area(right_col)).alias(
                "area_relative"
            ),
        )
        .with_columns(
            metric=(pl.col("area_intersection") / pl.col("area_relative"))
        )
        .with_columns((pl.col("metric") > threshold).alias(column_name))
    )


def sc_correspond_distance(
    lf: pl.LazyFrame,
    left_col: IntoExpr,
    right_col: IntoExpr,
    *,
    column_name: str = "corresponds",
    threshold: IntoExpr = 10,
) -> pl.LazyFrame:
    return lf.with_columns(
        metric=udf.distance(left_col, right_col)
    ).with_columns((pl.col("metric") < threshold).alias(column_name))


def sc_anti_join(
    origin: pl.LazyFrame,
    joined: pl.LazyFrame,
    select: Iterable[str],
    **join_kwargs,
) -> pl.LazyFrame:
    join_kwargs["how"] = "anti"

    return (
        origin.join(joined, **join_kwargs)
        .with_columns(
            foreign=pl.coalesce(
                pl.col("^foreign$"),
                pl.lit(
                    [],
                    dtype=pl.List(
                        pl.Struct(
                            {
                                "provider": pl.String(),
                                "key": pl.String(),
                            }
                        )
                    ),
                ),
            ),
        )
        .select(*select)
    )


def spatial_correspondence(
    left: pl.LazyFrame,
    right: pl.LazyFrame,
    /,
    left_args=JoinArgs(),
    right_args=JoinArgs(),
    use_distance=False,
) -> pl.LazyFrame:
    """Perform a spatial correspondence between two datasets.

    Parameters
    ----------
    left : pl.LazyFrame
        Primary data frame. Geometry and attributes take
        priority from this data frame.
    right : pl.LazyFrame
        Secondary data frame to join onto `left`.
    left_args : _type_, optional
        Join arguments for left data frame.
    right_args : _type_, optional
        Join arguments for right data frame.
    use_distance : bool, optional
        If True, use distance to correspond features rather than
        polygon overlap. Defaults to False.

    Returns
    -------
    pl.LazyFrame
        The lazy computation for corresponding `right` onto `left`.
    """
    lhs = sc_initialize_lazy(left, left_args, "_left")
    rhs = sc_initialize_lazy(right, right_args, "_right")

    udf_join_indices = udf.nearest if use_distance else udf.intersects
    udf_corresponds = (
        sc_correspond_distance if use_distance else sc_correspond_overlap
    )

    intersected = (
        # Join RHS geometry onto LHS so that RHS geometry is
        # in lazy context of LHS data frame.
        pl.concat(
            (
                lhs,
                rhs.select("geometry_right").filter(
                    pl.col("geometry_right").is_not_null()
                ),
            ),
            how="horizontal",
        )
        # Retrieve join indices for (non-null) LHS geometry.
        # `index_right` is a `list[i64]` column where for each
        # row `i`, each integer `j` of the list represents a
        # row index in RHS, such that
        #
        #     corresponds(LHS[i], RHS[j]) == true
        #
        .with_columns(
            pl.col("geometry_left")
            .drop_nulls()
            .pipe(udf_join_indices, pl.col("geometry_right"))
            .alias("index_right")
        )
        # Convert list column to integer column, adding more rows to data frame
        .explode("index_right")
        # RHS geometry is not needed anymore since we have the indices
        .drop("geometry_right")
        # We may have NULL LHS indices, since height(RHS) might be larger than height(LHS)
        .filter(pl.col("index_left").is_not_null())
        # inner-join RHS columns (including geometry) onto LHS based on indices
        .join(rhs, how="inner", on="index_right")
        .filter(pl.col("index_right").is_not_null())
        # Apply correspondence function (overlaps or distance) to ensure correspondence
        # and filter to only corresponding rows
        .pipe(
            udf_corresponds,
            pl.col("geometry_left"),
            pl.col("geometry_right"),
        )
        .filter("corresponds")
        # Below handles tied observations
        # > .filter(
        # >     pl.col("metric")
        # >     == (
        # >         pl.col("metric").min().over("id_left")
        # >         if use_distance
        # >         else pl.col("metric").max().over("id_left")
        # >     )
        # > )
        # Below does not handle tied observations
        # .group_by("index_left")
        # .agg(
        #     pl.all()
        #     .sort_by("metric", descending=not use_distance, nulls_last=True)
        #     .first()
        # )
        # ---------------
        .drop("corresponds", "geometry_right")
        # At this point, we have a data frame that contains only the
        # geometries between LHS and RHS that have some correspondence.
        # The following expressions clean the code to conform to the base schema.
        .with_columns(
            classification=sc_coalesce_attr("classification"),
            address=sc_coalesce_attr("address"),
            height=sc_coalesce_attr("height"),
            levels=sc_coalesce_attr("levels"),
            old_foreign=pl.concat_list(pl.selectors.starts_with("foreign")),
            new_foreign=pl.concat_list(
                pl.struct(
                    provider=pl.col("provider_right"),
                    key=pl.col("id_right"),
                    schema={
                        "provider": pl.String(),
                        "key": pl.String(),
                    },
                ).over("index_left")
            ),
        )
        .with_columns(foreign=pl.concat_list("old_foreign", "new_foreign"))
        .select(
            pl.col("id_left").alias("id"),
            pl.col("id_right").alias("tmp"),
            pl.col("provider_left").alias("provider"),
            *("classification", "address", "height", "levels", "foreign"),
            pl.col("geometry_left").alias("geometry"),
        )
    )

    select_cols = (
        "id",
        "provider",
        "classification",
        "address",
        "height",
        "levels",
        "foreign",
        "geometry",
    )

    # Get unmatched rows from left data frame
    left_missing = sc_anti_join(
        left,
        intersected,
        select_cols,
        on="id",
    )

    # Get unmatched rows from right data frame
    right_missing = sc_anti_join(
        right,
        intersected,
        select_cols,
        left_on="id",
        right_on="tmp",
    )

    intersected = intersected.drop("tmp")

    # Join all data frames together
    intersected = pl.concat(
        (intersected, left_missing, right_missing),
        how="vertical",
    )

    return intersected


def merge_footprints_and_addresses(
    footprints: pl.LazyFrame, addresses: pl.LazyFrame
) -> pl.LazyFrame:
    # Left = Footprints
    lhs = sc_initialize_lazy(footprints, JoinArgs(), "_left")
    # Right = Addresses
    rhs = sc_initialize_lazy(addresses, JoinArgs(), "_right")

    merged = (
        # Join footprint geometries onto addresses.
        pl.concat((rhs, lhs.select("geometry_left")), how="horizontal")
        .with_columns(
            index_left=pl.col("geometry_right").pipe(
                udf.nearest, pl.col("geometry_left")
            )
        )
        .explode("index_left")
        .drop("geometry_left")
        .filter(pl.col("index_left").is_not_null())
        .join(lhs, how="inner", on="index_left")
        .filter(pl.col("index_left").is_not_null())
        .pipe(
            sc_correspond_distance,
            pl.col("geometry_right"),
            pl.col("geometry_left"),
        )
        .filter("corresponds")
        .filter(pl.col("metric") == pl.col("metric").min().over("id_left"))
        .drop("corresponds")
        .with_columns(
            classification=sc_coalesce_attr("classification"),
            address=sc_coalesce_attr("address"),
            height=sc_coalesce_attr("height"),
            levels=sc_coalesce_attr("levels"),
            old_foreign=pl.concat_list(pl.selectors.starts_with("foreign")),
            new_foreign=pl.concat_list(
                pl.struct(
                    provider=pl.col("provider_left"),
                    key=pl.col("id_left"),
                    schema={
                        "provider": pl.String(),
                        "key": pl.String(),
                    },
                ).over("index_right")
            ),
        )
        .with_columns(foreign=pl.concat_list("old_foreign", "new_foreign"))
        .select(
            pl.col("id_right").alias("id"),
            pl.col("provider_right").alias("provider"),
            pl.col("id_left").alias("tmp"),
            *("classification", "address", "height", "levels", "foreign"),
            # When the point is ON the footprint surface, we use that (i.e. units),
            # otherwise, we use the footprint centroid (i.e. when address is in front of structure)
            pl.when(pl.col("metric") == 0)
            .then(pl.col("geometry_right"))
            .otherwise(pl.col("geometry_left"))
            .alias("geometry"),
        )
    )

    select_cols = (
        "id",
        "provider",
        "classification",
        "address",
        "height",
        "levels",
        "foreign",
        "geometry",
    )

    # Get unmatched rows from left data frame
    footprints_missing = sc_anti_join(
        footprints,
        merged,
        select_cols,
        left_on="id",
        right_on="tmp",
    )  # TODO to centroid

    # Get unmatched rows from right data frame
    addresses_missing = sc_anti_join(
        addresses,
        merged,
        select_cols,
        left_on="id",
        right_on="id",
    )

    merged = (
        pl.concat(
            (merged.drop("tmp"), footprints_missing, addresses_missing),
            how="vertical",
        )
        .with_columns(
            address=pl.col("address")
            .str.replace_all("\\s+dr$", " drive")
            .str.replace_all("\\s+st$", " street")
            .str.replace_all("\\s+ct$", " court")
            .str.replace_all("\\s+ln$", " lane")
            .str.replace_all("\\s+ave$", " avenue")
            .str.replace_all("\\s+rd$", " road")
            .map_elements(
                addr_normalize,
                return_dtype=pl.String(),
            )
            .replace("", pl.lit(None))
            .str.to_lowercase()
        )
        .with_columns(geometry=udf.centroid("geometry"))
        .select(pl.all().first().over("address", order_by="provider"))
        .unique(["id", "provider"])
        .with_columns(
            foreign=pl.concat_list(
                pl.col("foreign"),
                pl.concat_list(
                    pl.struct(
                        provider=pl.col("provider"),
                        key=pl.col("id"),
                        schema={
                            "provider": pl.String(),
                            "key": pl.String(),
                        },
                    )
                ),
            ),
            id=pl.col("geometry").pipe(udf.pluscodes),
        )
        .drop("provider")
    )

    return merged
