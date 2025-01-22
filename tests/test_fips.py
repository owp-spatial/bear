import pytest

from bear.core.fips import FIPS, USState, USCounty


def test_fips_state():
    with pytest.raises(KeyError):
        FIPS.state("00")

    state = FIPS.state("06")
    assert isinstance(state, USState)

    assert state.code == 6
    assert state.fips == "06"
    assert state.name == "California"
    assert state.abbreviation == "CA"
    assert state.bounds() == pytest.approx(
        (-2361582.28, 1242369.08, -1646659.54, 2455524.844), rel=1e-4
    )

    with pytest.raises(KeyError):
        state.county(0)

    with pytest.raises(KeyError):
        state.county("999")


def test_fips_county():
    state = FIPS.state("06")
    assert isinstance(state, USState)

    county = FIPS.county("06083")
    assert isinstance(county, USCounty)
    assert county.fips == "06083"
    assert county.code == 83
    assert county.state == state
    assert county.name == "Santa Barbara"
    assert county == state.county("083")


def test_fips_query_scalar():
    from shapely import Point

    county = FIPS.query(Point(-2173811.0344732204, 2020779.6029776197))
    assert county is not None
    assert county.state == FIPS.state("06")
    assert county.name == "Sacramento"
    assert county.fips == "06067"


def test_fips_query_array():
    from shapely import Point

    counties = FIPS.query(
        [
            # Sacramento Capitol
            Point(-2173811.0344732204, 2020779.6029776197),
            # Girvetz Hall, UCSB
            Point(-2152236.043699582, 1532664.814801817),
            # Outside CONUS
            Point(10407017.312142532, 3395226.1665611123),
        ]
    )

    assert counties[0] is not None
    assert counties[1] is not None
    assert counties[2] is None

    assert counties[0].name == "Sacramento"
    assert counties[0].fips == "06067"
    assert counties[0].state == FIPS.state("06")

    assert counties[1].name == "Santa Barbara"
    assert counties[1].fips == "06083"
    assert counties[1].state == FIPS.state("06")
