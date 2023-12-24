Example for running spark tests via pytest.
Point is to create faux tables that can then be tested by code running pyspark sql queries.

# basic install

1. install poetry
2. go:
```bash
poetry install
```

# run test

```bash
poetry run pytest .
```

# run test while building docker image

```bash
docker build .
```