from pathlib import Path

from polars import Expr
from polars.plugins import register_plugin_function
from polars._typing import IntoExpr

PLUGIN_PATH = Path(__file__).parent.parent


def intersection(lhs: IntoExpr, rhs: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="intersection",
        args=[lhs, rhs],
        is_elementwise=True,
    )


def distance(lhs: IntoExpr, rhs: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="distance",
        args=[lhs, rhs],
        is_elementwise=True,
    )


def area(expr: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="area",
        args=expr,
        is_elementwise=True,
    )


def intersects(lhs: IntoExpr, rhs: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="intersects",
        args=[lhs, rhs],
        is_elementwise=False,
    )


def corresponds(lhs: IntoExpr, rhs: IntoExpr) -> Expr:
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="corresponds",
        args=[lhs, rhs],
        is_elementwise=True,
    )
