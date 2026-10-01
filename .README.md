# Spark Project

A PySpark job that cleans transaction data, tested automatically with GitHub Actions.

## What it does
`clean_data(df)` in `pyspark_job.py`:
- removes rows where `amount <= 0`
- removes rows where `name` is NULL
- adds `amount_with_tax` = `amount * 1.20`

## Tests
Unit tests in `test_pyspark_job.py` (pytest) cover valid records, non-positive amounts, NULL names, and tax calculation,NULL amount, returned dataframe consists of 3 columns (names - amount - amount_with_tax)

## Run locally
Requires Java 17 and Python 3.10+.

    pip install -r requirements.txt
    pytest -v

## CI
`.github/workflows/ci.yml` runs the tests on every pull request (opened, updated, or reopened).