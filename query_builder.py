import marimo

__generated_with = "0.23.4"
app = marimo.App(width="medium", layout_file="layouts/query_builder.grid.json")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import polars.selectors as cs
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
def _(load_table, pl):
    acs5_2024 = load_table("https://api.census.gov/data/2024/acs/acs5/variables.json")
    acs5_2024 = acs5_2024.filter(~(pl.col('label') == 'Geography'))
    acs5_2024 = acs5_2024.select('name', 'label', 'concept', 'attributes')
    acs5_2024 = acs5_2024.sort('name')
    return (acs5_2024,)


@app.cell
def _(load_table, pl):
    acs5_2023 = load_table("https://api.census.gov/data/2023/acs/acs5/variables.json")
    acs5_2023 = acs5_2023.filter(~(pl.col('label') == 'Geography'))
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
    acs5 = acs5_2024.join(acs5_2023, on=acs5_2024.columns, how='inner', validate='1:1')
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
    acs5.filter(pl.col('concept').str.contains('Income')).sort('concept')
    return


@app.cell
def _(acs5, pl):
    (
        acs5
            .filter(
                pl.col('concept').str.contains('Median Value'),
            )
            .sort('concept')
    )
    return


@app.cell
def _(acs5, pl):
    (
        acs5
            .filter(
                pl.col('concept').str.contains('Population'),
                pl.col('concept').str.contains('Indian')
            )
            .sort('concept')
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Dashboard Components (2024)
    """)
    return


@app.cell
def _(acs5_2024, mo, pl):
    all_concept_options = acs5_2024.select(pl.col('concept').unique()).sort('concept').get_column('concept').to_list()
    all_concept_options = [x for x in all_concept_options if x]
    all_concept_select = mo.ui.multiselect(options=all_concept_options,)
    all_concept_select_widget = mo.vstack(
        [
            mo.md('Concept'),
             all_concept_select,
        ], 
    )
    return all_concept_select, all_concept_select_widget


@app.cell
def _(acs5_2024, all_concept_select, mo, pl):
    sub_concept_options = (
        acs5_2024
        .filter(pl.col('concept').is_in(all_concept_select.value))
        .select(pl.col('label').unique())
        .sort('label')
        .get_column('label')
        .to_list()
    )

    sub_concept_options = [o for o in sub_concept_options if o]

    sub_concept_select = mo.ui.multiselect(options=sub_concept_options,)
    sub_concept_select_widget = mo.vstack(
        [
            mo.md('Label'),
            sub_concept_select
        ],
    )

    print((
        acs5_2024
        .filter(pl.col('concept').is_in(all_concept_select.value))
        .sort('label')
        .get_column('label')
    ))
    return sub_concept_select, sub_concept_select_widget


@app.cell
def _(all_concept_select_widget, mo, sub_concept_select_widget):
    mo.vstack(
        [
            all_concept_select_widget,
            sub_concept_select_widget
        ], 
        justify="start",
        align="start",
        gap=0,
         )
    return


@app.cell
def _(acs5_2024, all_concept_select, pl, sub_concept_select):
    results_df = (
        acs5_2024
        .filter(pl.col('concept').is_in(all_concept_select.value))
        .filter(pl.col('label').is_in(sub_concept_select.value))
    )
    results = results_df.get_column('name')
    results = ','.join(results)
    results
    return results, results_df


@app.cell
def _(results):
    query = f'https://api.census.gov/data/2024/acs/acs5?get=NAME,{results}&for=county:*&in=state:48'
    query
    return (query,)


@app.cell
def _(query, requests, results):
    if results:
        data = requests.get(query).json()
    return (data,)


@app.cell
def _(pl, results_df):
    new_col_dict = (
        results_df
        .with_columns(
            new_col_name = pl.col("concept") + " - " + pl.col("label")
        )
        .select("name", "new_col_name")
        .to_dict(as_series=False)
    )

    new_col_dict = dict(zip(
        new_col_dict["name"],
        new_col_dict["new_col_name"]
    ))
    return (new_col_dict,)


@app.cell
def _(new_col_dict):
    new_col_dict
    return


@app.cell
def _(data, mo, new_col_dict, pl):
    new_df = pl.DataFrame(
        data[1:],
        schema=data[0],
        orient="row",
    ).sort("NAME", descending=True)

    new_df = (
        new_df
        .with_columns(
            pl.col(col).cast(pl.Int64)
            for col in new_col_dict.keys()
        )
        .filter(
            pl.col(col) > 0
            for col in new_col_dict.keys()
        )
        .rename({
            col: new_col_dict[col]
            for col in new_df.columns
            if col in new_col_dict
        })
    )


    mo.ui.table(new_df, wrapped_columns=new_df.columns)
    return (new_df,)


@app.cell
def _(new_df):
    new_df.columns
    return


@app.cell
def _():
    return


@app.cell
def _(acs5_2024, mo):
    table = mo.ui.table(acs5_2024, wrapped_columns=['label', 'concept'], page_size=5)
    table
    return


@app.cell
def _():
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
def _(pl, subject_df):
    (
        subject_df
            .filter(
                pl.col('concept').str.contains('Population'),
            )
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
