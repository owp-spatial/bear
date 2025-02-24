# Built Environment Asset Registry (BEAR)

**BEAR** is an integrated contintental-scale geospatial dataset for building entities.
This repository contains software for generating BEAR from known sources.

## Installation

The `bear` package can be installed via:

```bash
pip install git+https://github.com/owp-spatial/bear
```

## Development

We use [uv](https://docs.astral.sh/uv/) for package management. To setup a development environment,
first clone the repository and run the following to install the necessary dependencies:

```bash
uv sync
```

This project uses [maturin](https://github.com/PyO3/maturin) to build rust code alongside the python
package for [polars plugins](https://docs.pola.rs/user-guide/plugins/).

## License

This repository is licensed under the [Apache License 2.0](LICENSE).
