use polars::prelude::*;
use polars_arrow::array::Int64Array;
use polars_arrow::bitmap::utils::BitmapIter;
use polars_arrow::{bitmap::Bitmap, buffer::Buffer};

use geo::{Area, BoundingRect, Centroid, Convert, CoordsIter, Intersects, Point};
use geo_index::rtree::{sort::STRSort, RTree, RTreeBuilder, RTreeIndex};
use geo_traits::to_geo::ToGeoGeometry;
use geos::Geom;
use geozero::{CoordDimensions, ToGeo, ToGeos, ToWkb};
use wkb::reader::read_wkb;

use std::iter::Zip;
use std::slice::Iter;

pub struct GeoArray {
    pub values: Buffer<geo::Geometry>,
    pub bitmap: Bitmap,
    index: RTree<f64>,
}

impl GeoArray {
    pub fn iter(&self) -> Zip<Iter<'_, geo::Geometry>, BitmapIter> {
        self.values.iter().zip(self.bitmap.iter())
    }

    pub fn explode_multipoint(self) -> GeoArray {
        self.iter()
            .flat_map(|(g, ok)| g.coords_iter().zip(std::iter::repeat(ok)))
            .map(|(p, ok)| -> Option<geo::Geometry> {
                if ok {
                    Some(Point::from(p).to_geometry())
                } else {
                    None
                }
            })
            .collect::<Vec<Option<geo::Geometry>>>()
            .into()
    }

    pub fn explode_multipolygon(self) -> GeoArray {
        self.iter()
            .flat_map(|(g, ok)| {
                let mp: Result<geo::MultiPolygon, _> = g.convert().try_into();

                // Assume if the geometry is not a multipolygon, then it is a polygon.
                match mp {
                    Ok(p) => p.into_iter().zip(std::iter::repeat(ok)),
                    Err(_) => {
                        std::iter::once(TryInto::<geo::Polygon>::try_into(g.convert()).unwrap())
                            .collect::<Vec<geo::Polygon>>()
                            .into_iter()
                            .zip(std::iter::repeat(ok).into_iter())
                    }
                }
            })
            .map(
                |(poly, ok)| {
                    if ok {
                        Some(poly.to_geometry())
                    } else {
                        None
                    }
                },
            )
            .collect::<Vec<Option<geo::Geometry>>>()
            .into()
    }

    pub fn to_wkb(self) -> BinaryChunked {
        let mut builder = BinaryChunkedBuilder::new("".into(), self.values.len());

        for (geom, ok) in self.iter() {
            if ok {
                builder.append_value(geom.to_wkb(CoordDimensions::xy()).unwrap())
            } else {
                builder.append_null();
            }
        }

        builder.finish()
    }

    pub fn nearest_within_agg(&self, other: &GeoArray) -> ListChunked {
        self.iter()
            .map(|(g, ok)| {
                if ok {
                    let centroid = g.centroid().unwrap();
                    let (x, y) = centroid.x_y();
                    let query = other.index.neighbors(x, y, None, Some(20.0));

                    let matches: Vec<i64> = query
                        .into_iter()
                        .map(|i: u32| -> usize { i.try_into().unwrap() })
                        .filter(|i: &usize| {
                            g.to_geos()
                                .unwrap()
                                .distance(&other.values[*i].to_geos().unwrap())
                                .unwrap()
                                < 10.0
                        })
                        .map(|i: usize| -> i64 { i.try_into().unwrap() })
                        .collect();

                    Some(Int64Array::from_vec(matches).boxed())
                } else {
                    None
                }
            })
            .collect_ca_trusted_with_dtype(
                PlSmallStr::default(),
                DataType::List(DataType::Int64.boxed()),
            )
    }

    pub fn intersects_agg(&self, other: &GeoArray) -> ListChunked {
        self.iter()
            .map(|(g, ok)| {
                if ok {
                    let bbox = g.bounding_rect().unwrap();
                    let query = other.index.search_rect(&bbox);

                    let matches: Vec<i64> = query
                        .into_iter()
                        .map(|i: u32| -> usize { i.try_into().unwrap() })
                        .filter(|i: &usize| g.intersects(&other.values[*i]))
                        .map(|i: usize| -> i64 { i.try_into().unwrap() })
                        .collect();

                    Some(Int64Array::from_vec(matches).boxed())
                } else {
                    None
                }
            })
            .collect_ca_trusted_with_dtype(
                PlSmallStr::default(),
                DataType::List(DataType::Int64.boxed()),
            )
    }

    // pub fn intersects_elementwise(&self, other: &GeoArray) -> BooleanChunked {
    //     self.iter()
    //         .zip(other.iter())
    //         .map(|ab| match ab {
    //             ((a, true), (b, true)) => a.intersects(b),
    //             _ => false,
    //         })
    //         .collect_ca_trusted(PlSmallStr::default())
    // }

    pub fn intersection_elementwise(&self, other: &GeoArray) -> GeoArray {
        self.iter()
            .zip(other.iter())
            .map(|ab| match ab {
                ((a, true), (b, true)) => Some(
                    a.to_geos()
                        .unwrap()
                        .intersection(&b.to_geos().unwrap())
                        .unwrap()
                        .to_geo()
                        .unwrap(),
                ),
                _ => None,
            })
            .collect::<Vec<Option<geo::Geometry>>>()
            .into()
    }

    pub fn distance_elementwise(&self, other: &GeoArray) -> Float64Chunked {
        self.iter()
            .zip(other.iter())
            .map(|ab| match ab {
                ((a, true), (b, true)) => Some(
                    a.to_geos()
                        .unwrap()
                        .distance(&b.to_geos().unwrap())
                        .unwrap(),
                ),
                _ => None,
            })
            .collect_ca_trusted(PlSmallStr::default())
    }

    pub fn area(&self) -> Float64Chunked {
        self.iter()
            .map(|(g, ok)| if ok { Some(g.unsigned_area()) } else { None })
            .collect_ca_trusted(PlSmallStr::default())
    }

    pub fn centroid(mut self) -> GeoArray {
        self.values = self
            .values
            .iter()
            .map(|g| g.centroid().unwrap().to_geometry())
            .collect();

        self
    }
}

impl From<Vec<Option<geo::Geometry>>> for GeoArray {
    fn from(value: Vec<Option<geo::Geometry>>) -> Self {
        let mut values: Vec<geo::Geometry> = vec![];
        let mut bitmap: Vec<bool> = vec![];
        values.reserve_exact(value.len());
        bitmap.reserve_exact(value.len());

        let add_geom = |g: geo::Geometry, v: &mut Vec<geo::Geometry>, b: &mut Vec<bool>| {
            v.push(g);
            b.push(true);
        };

        let add_null = |v: &mut Vec<geo::Geometry>, b: &mut Vec<bool>| {
            v.push(geo::point! {x: f64::NAN, y: f64::NAN}.into());
            b.push(false);
        };

        let mut ngeoms: u32 = 0;
        for maybe_geom in value.into_iter() {
            match maybe_geom {
                Some(geom) => {
                    add_geom(geom, &mut values, &mut bitmap);
                    ngeoms += 1;
                }
                None => add_null(&mut values, &mut bitmap),
            }
        }

        let mut tree = RTreeBuilder::<f64>::new(ngeoms);
        for (g, ok) in values.iter().zip(bitmap.iter()) {
            if *ok {
                tree.add_rect(&g.bounding_rect().unwrap());
            }
        }

        GeoArray {
            values: values.into(),
            bitmap: bitmap.into(),
            index: tree.finish::<STRSort>(),
        }
    }
}

impl From<&BinaryChunked> for GeoArray {
    fn from(value: &BinaryChunked) -> Self {
        let mut values: Vec<geo::Geometry> = vec![];
        let mut bitmap: Vec<bool> = vec![];
        values.reserve_exact(value.len());
        bitmap.reserve_exact(value.len());

        let add_geom = |g: geo::Geometry, v: &mut Vec<geo::Geometry>, b: &mut Vec<bool>| {
            v.push(g);
            b.push(true);
        };

        let add_null = |v: &mut Vec<geo::Geometry>, b: &mut Vec<bool>| {
            v.push(geo::point! {x: f64::NAN, y: f64::NAN}.into());
            b.push(false);
        };

        let mut ngeoms: u32 = 0;
        for maybe_wkb in value.into_iter() {
            match maybe_wkb {
                Some(wkb) => match read_wkb(wkb) {
                    Ok(maybe_geom) => match maybe_geom.try_to_geometry() {
                        Some(geom) => {
                            add_geom(geom, &mut values, &mut bitmap);
                            ngeoms += 1;
                        }
                        None => add_null(&mut values, &mut bitmap),
                    },
                    Err(_) => add_null(&mut values, &mut bitmap),
                },
                None => add_null(&mut values, &mut bitmap),
            }
        }

        let mut tree = RTreeBuilder::<f64>::new(ngeoms);
        for (g, ok) in values.iter().zip(bitmap.iter()) {
            if *ok {
                tree.add_rect(&g.bounding_rect().unwrap());
            }
        }

        GeoArray {
            values: values.into(),
            bitmap: bitmap.into(),
            index: tree.finish::<STRSort>(),
        }
    }
}

impl TryFrom<Series> for GeoArray {
    type Error = PolarsError;

    fn try_from(value: Series) -> Result<Self, Self::Error> {
        Ok(value.binary()?.into())
    }
}

impl Into<BinaryChunked> for GeoArray {
    fn into(self) -> BinaryChunked {
        self.to_wkb()
    }
}

impl Into<Series> for GeoArray {
    fn into(self) -> Series {
        self.to_wkb().into_series()
    }
}
