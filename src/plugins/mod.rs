mod corresponds;
mod intersects;
mod utils;

use geos::{Geom, Geometry};
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use utils::GeoArray;

#[polars_expr(output_type=Binary)]
fn intersection(inputs: &[Series]) -> PolarsResult<Series> {
    let a = inputs[0].binary()?;
    let b = inputs[0].binary()?;

    let result: BinaryChunked = arity::broadcast_binary_elementwise_values(a, b, |a, b| {
        let a = Geometry::new_from_wkb(a).unwrap();
        let b = Geometry::new_from_wkb(b).unwrap();
        let c = a.intersection(&b).unwrap().to_wkb().unwrap();

        Into::<Vec<u8>>::into(c)
    });

    Ok(result.into_series())
}

#[polars_expr(output_type=Float64)]
fn distance(inputs: &[Series]) -> PolarsResult<Series> {
    let a = inputs[0].binary()?;
    let b = inputs[0].binary()?;

    let result: Float64Chunked = arity::broadcast_binary_elementwise_values(a, b, |a, b| {
        let a = Geometry::new_from_wkb(a).unwrap();
        let b = Geometry::new_from_wkb(b).unwrap();
        a.distance(&b).unwrap()
    });

    Ok(result.into_series())
}

#[polars_expr(output_type=Float64)]
fn area(inputs: &[Series]) -> PolarsResult<Series> {
    let input: GeoArray = inputs[0].binary()?.into();

    let result: Vec<f64> = input.data.into_iter().map(|geom| {
        match geom {
            Some(g) => g.area().unwrap_or(0.0),
            None => 0.0
        }
    }).collect();

    Ok(Float64Chunked::from_vec("".into(), result).into_series())
}
