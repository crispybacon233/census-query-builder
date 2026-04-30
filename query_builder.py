import marimo

__generated_with = "0.23.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import requests

    return mo, pl, requests


@app.cell
def _(pl, requests):

    url = "https://api.census.gov/data/2024/acs/acs5/variables.json"

    data = requests.get(url).json()["variables"]

    variables_df = (
        pl.from_dicts(
            [
                {"name": name, **meta}
                for name, meta in data.items()
            ],
            infer_schema_length=None,
        )
    )
    return (variables_df,)


@app.cell
def _(mo, variables_df):
    mo.ui.table(variables_df)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
