import polars as pl
from polars._typing import IntoExpr

from dataclasses import dataclass
from typing import Iterable

import bear._plugins as udf


@dataclass(slots=True)
class JoinArgs:
    id: str = "id"
    geometry: str = "geometry"


def sc_initialize_listcol(name: str):
    return pl.coalesce(
        pl.selectors.matches(name),
        pl.lit([], pl.List(pl.String())),
    )


def sc_initialize_lazy(
    lf: pl.LazyFrame,
    args: JoinArgs,
    suffix: str,
    *,
    row_index_name="__row_index__",
) -> pl.LazyFrame:
    return (
        lf.with_row_index(row_index_name)
        .with_columns(
            foreign=sc_initialize_listcol("foreign"),
            providers=sc_initialize_listcol("providers"),
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
    column_name: str = "__corresponds__",
    threshold: IntoExpr = 0.3,
) -> pl.LazyFrame:
    return lf.with_columns(
        # Intersection Area
        udf.area(udf.intersection(left_col, right_col)).alias(
            "area_intersection"
        ),
        # Relative Area
        pl.min_horizontal(udf.area(left_col), udf.area(right_col)).alias(
            "area_relative"
        ),
    ).with_columns(
        (
            (pl.col("area_intersection") / pl.col("area_relative")) > threshold
        ).alias(column_name)
    )


def sc_correspond_distance(
    lf: pl.LazyFrame,
    left_col: IntoExpr,
    right_col: IntoExpr,
    *,
    column_name: str = "__corresponds__",
    threshold: IntoExpr = 10,
) -> pl.LazyFrame:
    return lf.with_columns(
        (udf.distance(left_col, right_col) < threshold).alias(column_name)
    )


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
            foreign=sc_initialize_listcol("foreign"),
            providers=sc_initialize_listcol("providers"),
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

    intersected = (
        lhs.with_columns(
            udf.intersects(
                pl.col("geometry_left"),
                # TODO(justin): we have to collect here because
                # out-of-context columns are not pullable from LazyFrames.
                # Therefore, this needs to be a materialized Series (as far as I know).
                #
                # Maybe there is a better way to optimize this so that we can
                # fully express this in terms of lazy computation?
                rhs.select("geometry_right").collect(streaming=True)[
                    "geometry_right"
                ],
            ).alias("__row_index___right")
        )
        .explode("__row_index___right")
        .join(
            rhs,
            how="left",
            on="__row_index___right",
        )
    )

    if use_distance:
        intersected = sc_correspond_distance(
            intersected, pl.col("geometry_left"), pl.col("geometry_right")
        )
    else:
        intersected = sc_correspond_overlap(
            intersected, pl.col("geometry_left"), pl.col("geometry_right")
        )

    intersected = (
        intersected.filter("__corresponds__")
        .drop("__corresponds__", "geometry_right")
        .with_columns(
            classification=sc_coalesce_attr("classification"),
            address=sc_coalesce_attr("address"),
            height=sc_coalesce_attr("height"),
            levels=sc_coalesce_attr("levels"),
            foreign=pl.concat_list(
                pl.selectors.starts_with("foreign"), pl.col("id_right")
            ),
            providers=pl.concat_list(
                pl.selectors.starts_with("providers"), pl.col("provider_right")
            ),
        )
        .select(
            pl.col("id_left").alias("id"),
            pl.col("id_right").alias("tmp"),
            pl.col("provider_left").alias("provider"),
            *(
                "classification",
                "address",
                "height",
                "levels",
                "foreign",
                "providers",
            ),
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
        "providers",
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

    # Join all data frames together
    intersected = pl.concat(
        (intersected.drop("tmp"), left_missing, right_missing),
        how="vertical",
    )

    return intersected
