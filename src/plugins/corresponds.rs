use geos::{Geom, Geometry, GeometryTypes};
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

fn corresponds_footprints(a: &Geometry, b: &Geometry) -> bool {
    let a_area = a.area().unwrap_or(f64::MAX);
    let b_area = b.area().unwrap_or(f64::MAX);
    let rel_area = a_area.min(b_area);
    let intersection = a.intersection(b).unwrap();
    let int_area = intersection.area().unwrap_or(0.0);

    (int_area / rel_area) > 0.3
}

// a is the point, b is footprint/point
fn corresponds_address(a: &Geometry, b: &Geometry) -> bool {
    a.distance(b).unwrap_or(f64::MAX) < 10.0
}

#[polars_expr(output_type=Boolean)]
fn corresponds(inputs: &[Series]) -> PolarsResult<Series> {
    if inputs.len() != 2 {
        return Err(PolarsError::InvalidOperation(
            format!(
                "Operation `corresponds` requires 2 series, but received {}",
                inputs.len()
            )
            .into(),
        ));
    }

    let a = inputs[0].binary()?;
    let b = inputs[1].binary()?;

    let result: BooleanChunked = arity::broadcast_binary_elementwise_values(a, b, |a, b| {
        let a = Geometry::new_from_wkb(a);
        let b = Geometry::new_from_wkb(b);
        match (a, b) {
            (Ok(a), Ok(b)) => {
                let a_type = Geometry::geometry_type(&a);
                let b_type = Geometry::geometry_type(&b);
                match (a_type, b_type) {
                    (GeometryTypes::Point, _) => corresponds_address(&a, &b),
                    (_, GeometryTypes::Point) => corresponds_address(&b, &a),
                    (GeometryTypes::Polygon, GeometryTypes::Polygon)
                    | (GeometryTypes::MultiPolygon, GeometryTypes::Polygon)
                    | (GeometryTypes::Polygon, GeometryTypes::MultiPolygon)
                    | (GeometryTypes::MultiPolygon, GeometryTypes::MultiPolygon) => {
                        corresponds_footprints(&a, &b)
                    }
                    (_, _) => false,
                }
            }
            (_, _) => false,
        }
    });

    Ok(result.into_series())
}
