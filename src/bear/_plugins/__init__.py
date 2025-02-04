from pathlib import Path

from polars import Expr
from polars.plugins import register_plugin_function
from polars._typing import IntoExpr

PLUGIN_PATH = Path(__file__).parent.parent


def intersects(lhs: IntoExpr, rhs: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="binary_intersects_aggregate",
        args=[lhs, rhs],
        is_elementwise=False,
    )


def nearest(lhs: IntoExpr, rhs: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="binary_nearest_aggregate",
        args=[lhs, rhs],
        is_elementwise=False,
    )


def intersection(lhs: IntoExpr, rhs: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="binary_intersection_elementwise",
        args=[lhs, rhs],
        is_elementwise=True,
    )


def area(expr: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="unary_area_elementwise",
        args=expr,
        is_elementwise=True,
    )


def distance(lhs: IntoExpr, rhs: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="binary_distance_elementwise",
        args=[lhs, rhs],
        is_elementwise=True,
    )


def centroid_x(expr: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="unary_x",
        args=expr,
        is_elementwise=True,
    )


def centroid_y(expr: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="unary_y",
        args=expr,
        is_elementwise=True,
    )


def centroid(expr: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="unary_centroid",
        args=expr,
        is_elementwise=True,
    )


def explode_multipoint(expr: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="unary_explode_multipoint",
        args=expr,
        is_elementwise=True,
        changes_length=True,
    )


def explode_multipolygon(expr: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="unary_explode_multipolygon",
        args=expr,
        is_elementwise=True,
        changes_length=True,
    )


def pluscodes(expr: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="unary_pluscode",
        args=expr,
        is_elementwise=True,
    )
