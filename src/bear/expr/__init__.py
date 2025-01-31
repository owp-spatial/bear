import polars as pl
from typing import Final

from bear._plugins import area, distance, intersects, intersection

NULL: Final = pl.lit(None)
EMPTY_STR: Final = pl.lit("")
NULL_UUID: Final = pl.lit("{00000000-0000-0000-0000-000000000000}")


def null_if_empty_str(string: pl.Expr) -> pl.Expr:
    return pl.when(string.eq(EMPTY_STR)).then(NULL).otherwise(string)


def normalize_str(string: pl.Expr) -> pl.Expr:
    return string.str.strip_chars().str.to_lowercase()


def spatial_correspondence(
    left: pl.LazyFrame,
    right: pl.LazyFrame,
    /,
    left_geometry="geometry",
    right_geometry="geometry",
    left_id="id",
    right_id="id",
    use_distance=False,
) -> pl.LazyFrame:
    lhs = (
        left.with_row_index("__row_index__")
        .with_columns(
            foreign=pl.coalesce(
                pl.selectors.matches("foreign"),
                pl.lit([], pl.List(pl.String())),
            ),
            providers=pl.coalesce(
                pl.selectors.matches("providers"),
                pl.lit([], pl.List(pl.String())),
            ),
        )
        .rename({left_geometry: "geometry", left_id: "id"})
        .select(pl.all().name.suffix("_left"))
    )

    rhs = (
        right.with_row_index("__row_index__")
        .with_columns(
            foreign=pl.coalesce(
                pl.selectors.matches("foreign"),
                pl.lit([], pl.List(pl.String())),
            ),
            providers=pl.coalesce(
                pl.selectors.matches("providers"),
                pl.lit([], pl.List(pl.String())),
            ),
        )
        .rename({right_geometry: "geometry", right_id: "id"})
        .select(pl.all().name.suffix("_right"))
    )

    intersected = (
        lhs.with_columns(
            intersects(
                pl.col("geometry_left"),
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
        intersected = intersected.with_columns(
            (
                distance(pl.col("geometry_left"), pl.col("geometry_right")) < 10
            ).alias("__corresponds__")
        )

    else:
        intersected = intersected.with_columns(
            area(
                intersection(
                    pl.col("geometry_left"),
                    pl.col("geometry_right"),
                )
            ).alias("area_both"),
            pl.min_horizontal(
                area(pl.col("geometry_left")),
                area(pl.col("geometry_right")),
            ).alias("area_relative"),
        ).with_columns(
            ((pl.col("area_both") / pl.col("area_relative")) > 0.3).alias(
                "__corresponds__"
            )
        )

    intersected = (
        intersected.filter("__corresponds__")
        .drop("__corresponds__", "geometry_right")
        .with_columns(
            classification=pl.coalesce(
                pl.selectors.starts_with("classification")
            ),
            address=pl.coalesce(pl.selectors.starts_with("address")),
            height=pl.coalesce(pl.selectors.starts_with("height")),
            levels=pl.coalesce(pl.selectors.starts_with("levels")),
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

    left_missing = (
        left.join(intersected, on="id", how="anti")
        .with_columns(
            foreign=pl.coalesce(
                pl.selectors.matches("foreign"),
                pl.lit([], pl.List(pl.String())),
            ),
            providers=pl.coalesce(
                pl.selectors.matches("providers"),
                pl.lit([], pl.List(pl.String())),
            ),
        )
        .select(
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
    )

    right_missing = (
        right.join(intersected, left_on="id", right_on="tmp", how="anti")
        .with_columns(
            foreign=pl.coalesce(
                pl.selectors.matches("foreign"),
                pl.lit([], pl.List(pl.String())),
            ),
            providers=pl.coalesce(
                pl.selectors.matches("providers"),
                pl.lit([], pl.List(pl.String())),
            ),
        )
        .select(
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
    )

    intersected = pl.concat(
        (intersected.drop("tmp"), left_missing, right_missing), how="vertical"
    )

    return intersected
