mod geoarray;

use geo::{proj::Proj, Centroid, Convert, Point, Transform};
use geoarray::GeoArray;

use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

fn unary_input(inputs: &[Series]) -> PolarsResult<GeoArray> {
    let a: &BinaryChunked = inputs[0].binary()?;
    Ok(a.into())
}

fn binary_inputs(inputs: &[Series]) -> PolarsResult<(GeoArray, GeoArray)> {
    let a: &BinaryChunked = inputs[0].binary()?;
    let b: &BinaryChunked = inputs[1].binary()?;

    Ok((a.into(), b.into()))
}

fn intersects_output_type(fields: &[Field]) -> PolarsResult<Field> {
    let field = Field::new(
        fields[0].name.clone(),
        DataType::List(DataType::Int64.boxed()),
    );

    Ok(field.clone())
}

/// Perform an aggregated intersects operation between two
/// Polars Series. The resulting Series has equal length to
/// the left input, and a spatial index of the right Series is used
/// to determine intersecting geometries.
#[polars_expr(output_type_func=intersects_output_type)]
fn binary_intersects_aggregate(inputs: &[Series]) -> PolarsResult<Series> {
    let (a, b) = binary_inputs(inputs)?;
    let series = a.intersects_agg(&b).into_series();

    let a_len = a.values.len();
    let b_len = b.values.len();
    let c_len = a_len.max(b_len);
    let remaining = c_len.abs_diff(a_len);

    series.extend_constant(AnyValue::Null, remaining)
}

/// Perform an aggregated nearest neighbor operation between
/// two Polars Series. The resulting Series has equal length to
/// the left input, and a spatial index of the right Series is used
/// to determine neighboring geometries.
#[polars_expr(output_type_func=intersects_output_type)]
fn binary_nearest_aggregate(inputs: &[Series]) -> PolarsResult<Series> {
    let (a, b) = binary_inputs(inputs)?;
    let series = a.nearest_within_agg(&b).into_series();

    let a_len = a.values.len();
    let b_len = b.values.len();
    let c_len = a_len.max(b_len);
    let remaining = c_len.abs_diff(a_len);

    series.extend_constant(AnyValue::Null, remaining)
}

/// Perfom an elementwise intersection between equal length WKB Series.
#[polars_expr(output_type=Binary)]
fn binary_intersection_elementwise(inputs: &[Series]) -> PolarsResult<Series> {
    let (a, b) = binary_inputs(inputs)?;
    Ok(a.intersection_elementwise(&b).into())
}

/// Compute the area of each geometry in a WKB Series.
#[polars_expr(output_type=Float64)]
fn unary_area_elementwise(inputs: &[Series]) -> PolarsResult<Series> {
    Ok(unary_input(inputs)?.area().into_series())
}

/// Compute the (elementwise) distance between geometries.
#[polars_expr(output_type=Float64)]
fn binary_distance_elementwise(inputs: &[Series]) -> PolarsResult<Series> {
    let (a, b) = binary_inputs(inputs)?;
    Ok(a.distance_elementwise(&b).into_series())
}

#[polars_expr(output_type=Float64)]
fn unary_x(inputs: &[Series]) -> PolarsResult<Series> {
    unary_input(inputs)?
        .iter()
        .map(|(g, ok)| {
            if ok {
                Ok(g.centroid().unwrap().x())
            } else {
                Ok(f64::NAN)
            }
        })
        .collect()
}

#[polars_expr(output_type=Float64)]
fn unary_y(inputs: &[Series]) -> PolarsResult<Series> {
    unary_input(inputs)?
        .iter()
        .map(|(g, ok)| {
            if ok {
                Ok(g.centroid().unwrap().y())
            } else {
                Ok(f64::NAN)
            }
        })
        .collect()
}

#[polars_expr(output_type=Binary)]
fn unary_centroid(inputs: &[Series]) -> PolarsResult<Series> {
    Ok(unary_input(inputs)?.centroid().to_wkb().into_series())
}

#[polars_expr(output_type=Binary)]
fn unary_explode_multipoint(inputs: &[Series]) -> PolarsResult<Series> {
    Ok(unary_input(inputs)?
        .explode_multipoint()
        .to_wkb()
        .into_series())
}

#[polars_expr(output_type=Binary)]
fn unary_explode_multipolygon(inputs: &[Series]) -> PolarsResult<Series> {
    Ok(unary_input(inputs)?
        .explode_multipolygon()
        .to_wkb()
        .into_series())
}

#[polars_expr(output_type=String)]
fn unary_pluscode(inputs: &[Series]) -> PolarsResult<Series> {
    let proj = Proj::new_known_crs("EPSG:5070", "EPSG:4326", None).unwrap();

    let result: StringChunked = unary_input(inputs)?
        .centroid()
        .iter()
        .map(|(g, ok)| {
            if ok {
                let pt: Point = g.convert().try_into().unwrap();
                let latlon = pt.transformed(&proj).unwrap().x_y();
                Some(open_location_code::encode(latlon.into(), 13))
            } else {
                None
            }
        })
        .collect();

    Ok(result.into_series())
}
