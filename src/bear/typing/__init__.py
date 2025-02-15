from __future__ import annotations

from typing import (
    Generator,
    Optional,
    Protocol,
    runtime_checkable,
)

from polars import LazyFrame
from pyarrow import RecordBatch as ArrowRecordBatch
from pyarrow import Schema as ArrowSchema

from bear.core.fips import USCounty

ArrowBatchGenerator = Generator[ArrowRecordBatch, None, None]


@runtime_checkable
class Provider(Protocol):
    """Provider Protocol

    Provides an interface for functions a module should implement
    to be considered a Provider.
    """

    @classmethod
    def epsg(cls) -> int:
        """Retrieve the EPSG code for this provider's data

        Returns
        -------
        int
            EPSG code for the underlying data.
        """
        ...

    @classmethod
    def schema(cls) -> Optional[ArrowSchema]:
        """Optionally retrieve the schema of this provider's data

        Returns
        -------
        Optional[ArrowSchema]
            If supported, returns the schema of the provider's data.
        """
        ...

    @classmethod
    def read(cls, county: USCounty, *args, **kwargs) -> ArrowBatchGenerator:
        """Read provider data via a pyarrow.RecordBatch generator

        Parameters
        ----------
        county : USCounty
            The area of interest (AOI) to pull record batches from.
        *args
            Positional arguments passed to implementation.
        **kwargs
            Keyword arguments passed to implementation.

        Returns
        -------
        ArrowBatchGenerator
            A generator yielding pyarrow.RecordBatch objects.
        """
        ...

    @classmethod
    def conform(cls, lf: LazyFrame, *args, **kwargs) -> LazyFrame:
        """Conform function definition

        Parameters
        ----------
        lf : polars.LazyFrame
            Deferred evaluation polars data frame to apply
            the implemented conform function against.
        *args
            Positional arguments passed to implementation.
        **kwargs
            Keyword arguments passed to implementation.

        Returns
        -------
        polars.LazyFrame
            A new lazy frame with the conform expressions applied.
        """
        ...
