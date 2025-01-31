use geos::{Geom, SpatialIndex};
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use pyo3_polars::export::polars_core::utils::arrow::array::Int64Array;

use crate::plugins::utils::GeoArray;

fn intersects_output(_: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        "".into(),
        DataType::List(DataType::Int64.boxed()),
    ))
}

/// Performs a spatial intersects operation between two series.
/// Argument `inputs` must be exactly 2 Polars Series of WKB.
#[polars_expr(output_type_func=intersects_output)]
fn intersects(inputs: &[Series]) -> PolarsResult<Series> {
    if inputs.len() != 2 {
        return Err(PolarsError::InvalidOperation(
            format!(
                "Operation `intersects` requires 2 series, but received {}",
                inputs.len()
            )
            .into(),
        ));
    }

    let y: GeoArray = inputs[1].binary()?.into();
    let y_tree = y.tree()?;

    let x: GeoArray = inputs[0].binary()?.into();
    let mut all_indices: Vec<Series> = vec![];

    for maybe_x_geom in x.data.iter() {
        let mut values: Vec<i64> = vec![];
        let mut bitmap: Vec<bool> = vec![];

        match maybe_x_geom {
            Some(x_geom) => {
                let x_ref = x_geom.as_ref();
                y_tree.query(x_ref, |node| {
                    if node.geometry.intersects(x_ref).is_ok() {
                        values.push(node.index.try_into().unwrap());
                        bitmap.push(true);
                    }
                })
            }
            None => (),
        }

        let array = Int64Array::try_new(ArrowDataType::Int64, values.into(), Some(bitmap.into()))?;
        all_indices.push(Series::from_arrow("".into(), array.boxed())?);
    }

    return Ok(ListChunked::from_iter(all_indices.into_iter()).into_series());
}
