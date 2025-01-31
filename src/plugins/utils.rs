use std::rc::Rc;

use geos::{Geometry, SpatialIndex, STRtree};
use polars::prelude::*;

pub struct TreeNode {
    pub geometry: Rc<Geometry>,
    pub index: usize,
}

pub struct GeoArray {
    pub data: Vec<Option<Rc<Geometry>>>,
}

impl From<&BinaryChunked> for GeoArray {
    fn from(value: &BinaryChunked) -> Self {
        GeoArray {
            data: value
                .iter()
                .map(|maybe_wkb| match maybe_wkb {
                    Some(wkb) => match Geometry::new_from_wkb(wkb) {
                        Ok(g) => Some(Rc::new(g)),
                        Err(_) => None,
                    },
                    None => None,
                })
                .collect(),
        }
    }
}

impl GeoArray {
    // TODO(justin): STRtree vs Flatbush?
    pub fn tree(self) -> PolarsResult<STRtree<TreeNode>> {
        let mut strtree = match STRtree::with_capacity(self.data.len()) {
            Err(e) => Err(PolarsError::ComputeError(e.to_string().into())),
            Ok(t) => Ok(t),
        }?;

        for (pos, geom) in self.data.iter().enumerate() {
            if geom.is_none() {
                continue;
            }

            let reference: &Rc<Geometry> = &geom.as_ref().unwrap();
            strtree.insert(
                reference.as_ref(),
                TreeNode {
                    geometry: reference.clone(),
                    index: pos,
                },
            );
        }

        Ok(strtree)
    }
}

