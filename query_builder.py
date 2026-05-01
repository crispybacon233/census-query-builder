import marimo

__generated_with = "0.23.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import duckdb
    import requests

    return duckdb, mo, pl, requests


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # ACS 5 Year
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Detailed Variable Data
    """)
    return


@app.cell
def _(pl, requests):
    def load_table(url: str) -> pl.DataFrame:
        data = requests.get(url).json()["variables"]
        return (
            pl.from_dicts(
                [
                    {"name": name, **meta}
                    for name, meta in data.items()
                ],
                infer_schema_length=None,
            )
        )

    return (load_table,)


@app.cell
def _(load_table):
    acs5_2024 = load_table("https://api.census.gov/data/2024/acs/acs5/variables.json")
    acs5_2024 = acs5_2024.select('name', 'label', 'concept', 'attributes')
    acs5_2024 = acs5_2024.sort('name')
    return (acs5_2024,)


@app.cell
def _(load_table):
    acs5_2023 = load_table("https://api.census.gov/data/2023/acs/acs5/variables.json")
    acs5_2023 = acs5_2023.select('name', 'label', 'concept', 'attributes')
    acs5_2023 = acs5_2023.sort('name')
    return (acs5_2023,)


@app.cell
def _(acs5_2023, acs5_2024):
    acs5_2023.write_parquet('data/acs5_2023.parquet')
    acs5_2024.write_parquet('data/acs5_2024.parquet')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Compare 2024 vs 2023 Data
    Check what data overlaps and what data does not overlap
    """)
    return


@app.cell
def _(duckdb):
    con = duckdb.connect()
    return (con,)


@app.cell
def _(acs5_2023, acs5_2024):
    acs5 = acs5_2024.join(acs5_2023, on=acs5_2024.columns, how='inner')
    return (acs5,)


@app.cell
def _(acs5_2024, con):
    cols_str = ", ".join([f'"{c}"' for c in acs5_2024.columns])

    con.sql(f"""
    SELECT * 
    FROM acs5_2024
    JOIN acs5_2023 USING ({cols_str})
    """).pl()
    return


@app.cell
def _(acs5):
    acs5
    return


@app.cell
def _(acs5, pl):
    # Income Filter
    acs5.filter(pl.col('concept').str.contains('Veteran')).unique('concept').sort('concept')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Subject Table
    """)
    return


@app.cell
def _(load_table):
    subject_df = load_table('https://api.census.gov/data/2024/acs/acs5/subject/variables.json')
    return (subject_df,)


@app.cell
def _(subject_df):
    subject_df
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
