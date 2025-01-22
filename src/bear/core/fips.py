from __future__ import annotations

import bear.core.static

from importlib.resources import files
from typing import Any, Generator, overload, Sequence, Optional
from itertools import islice
from shapely import (
    Geometry,
    coverage_union_all,
    bounds,
    STRtree,
    centroid,
)

import geopandas as gpd


class USCounty:
    __slots__ = ("_code", "_name", "_state", "_geometry")

    def __init__(
        self, code: int, name: str, geometry: Geometry, state: USState
    ):
        self._code = code
        self._name = name
        self._geometry = geometry
        self._state = state

        # Link the state to this county
        state._counties[code] = self

    @property
    def fips(self) -> str:
        return f"{self._state.fips}{str(self._code).rjust(3, '0')}"

    @property
    def code(self) -> int:
        return self._code

    @property
    def name(self) -> str:
        return self._name

    @property
    def geometry(self) -> Geometry:
        return self._geometry

    @property
    def state(self) -> USState:
        return self._state

    def bounds(self) -> tuple[float, float, float, float]:
        return tuple(x.item() for x in bounds(self.geometry))

    def __str__(self) -> str:
        return self.fips

    def __repr__(self) -> str:
        return f"USCounty({repr(self.code)}, {repr(self.name)}, {repr(self.geometry)}, {repr(self.state)})"

    def __eq__(self, other) -> bool:
        if isinstance(other, USCounty):
            return (
                self.state.code == other.state.code and self.code == other.code
            )
        elif isinstance(other, str) and len(other) == 5:
            return self.fips == other
        else:
            return False


class USState:
    __slots__ = ("_code", "_name", "_abbr", "_counties")

    def __init__(self, code: int, name: str, abbreviation: str):
        self._code = code
        self._name = name
        self._abbr = abbreviation
        self._counties: dict[int, USCounty] = {}

    @property
    def fips(self) -> str:
        return str(self._code).rjust(2, "0")

    @property
    def code(self) -> int:
        return self._code

    @property
    def name(self) -> str:
        return self._name

    @property
    def abbreviation(self) -> str:
        return self._abbr

    @property
    def geometry(self) -> Geometry:
        return coverage_union_all([x.geometry for x in self._counties.values()])

    def county(self, code_or_countyfp: int | str) -> USCounty:
        if isinstance(code_or_countyfp, str) and len(code_or_countyfp) == 5:
            code_or_countyfp = code_or_countyfp[2:5]

        return self._counties[int(code_or_countyfp)]

    def bounds(self) -> tuple[float, float, float, float]:
        return tuple(x.item() for x in bounds(self.geometry))

    def itercounties(self) -> Generator[USCounty, None, None]:
        return (county for county in self._counties.values())

    def __str__(self) -> str:
        return self.fips

    def __repr__(self) -> str:
        return f"USState({repr(self.code)}, {repr(self.name)}, {repr(self.abbreviation)})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, USState):
            return self.code == other.code
        elif isinstance(other, str):
            return (
                self.fips == other
                or self.name == other
                or self.abbreviation == other
            )
        elif isinstance(other, int):
            return self.code == other
        else:
            return False


class FIPS:
    _states: dict[int, USState] = {}
    _stree: STRtree

    @classmethod
    def epsg(cls) -> int:
        return 5070

    @classmethod
    def initialized(cls) -> bool:
        return len(cls._states.keys()) > 0

    @classmethod
    def initialize(cls) -> None:
        if cls.initialized():
            return

        path = f"GeoJSONSeq:/vsigzip/{str(files(bear.core.static) / 'fips.geojson.gz')}"
        gdf = gpd.read_file(path).to_crs(epsg=cls.epsg)
        for row in gdf.itertuples(index=False, name=None):
            # (0, fips); (1, name); (2, state); (3, abbr); (4, geometry)
            statefp: int = int(row[0][:2])

            try:
                state = cls._states[statefp]
            except KeyError:
                state = USState(statefp, row[2], row[3])
                cls._states[statefp] = state

            # No need to assign this, as calling its construtor
            # will link it into the state object (i.e. cls._states -> State -> County)
            USCounty(int(row[0][2:5]), row[1], row[4], state)

        cls._stree = STRtree(gdf.geometry, node_capacity=25)

    @classmethod
    def iterstates(cls) -> Generator[USState, None, None]:
        return (state for state in cls._states.values())

    @classmethod
    def itercounties(cls) -> Generator[USCounty, None, None]:
        for state in cls.iterstates():
            yield from state.itercounties()

    @classmethod
    @overload
    def query(cls, geometry: Geometry) -> Optional[USCounty]: ...

    @classmethod
    @overload
    def query(
        cls, geometry: Sequence[Geometry]
    ) -> Sequence[Optional[USCounty]]: ...

    @classmethod
    def query(
        cls, geometry: Geometry | Sequence[Geometry]
    ) -> Optional[USCounty] | Sequence[Optional[USCounty]]:
        indices = cls._stree.query(centroid(geometry), predicate="intersects")

        # Scalar Case
        if isinstance(geometry, Geometry):
            gen = cls.itercounties()
            return next(islice(gen, indices[0], None))
        else:
            # Array Case
            # TODO(justin): this is inefficient since it iterates the generator
            #               on each index -- might be better to sort, islice, reorder
            result: list[USCounty | None] = [None] * len(geometry)
            for i, idx in indices.T.tolist():
                gen = cls.itercounties()
                result[i] = next(islice(gen, idx, None))

        return result

    @staticmethod
    def state(key: str) -> USState:
        assert len(key) == 2
        FIPS.initialize()
        return FIPS._states[int(key)]

    @staticmethod
    def county(key: str) -> USCounty:
        assert len(key) == 5
        state = FIPS.state(key[:2])
        return state.county(key)

    @staticmethod
    def get(key: str) -> USState | USCounty:
        return FIPS.state(key) if len(key) == 2 else FIPS.county(key)

    def __getitem__(self, key: str) -> USState | USCounty:
        return self.get(key)
