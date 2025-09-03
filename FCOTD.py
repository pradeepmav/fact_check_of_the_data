import polars as pl
import pandas as pd

def find_df_name(df):
   name = [name for name, obj in globals().items() if id(obj) == id(df)]
   return name[0] if name else None

def fact_check_of_the_data(pldataframe):
    vardtypes = pl.DataFrame(zip(pldataframe.columns, pldataframe.dtypes)).rename(
        {"column_0": "Variable_Name", "column_1": "D_Type"}
    )

    varnonmissing = pldataframe.count().transpose(
        include_header=True,
        header_name="Variable_Name",
        column_names=["No_of_Non_Missing_Values"],
    )

    varmissing = pldataframe.null_count().transpose(
        include_header=True,
        header_name="Variable_Name",
        column_names=["No_of_Missing_Values"],
    )

    vardistinct = pldataframe.select(pl.all().n_unique()).transpose(
        include_header=True,
        header_name="Variable_Name",
        column_names=["Distinct_Values"],
    )

    varmin = pldataframe.min().transpose(
        include_header=True, 
        header_name="Variable_Name", 
        column_names=["Min"]
    )

    varmax = pldataframe.max().transpose(
        include_header=True,
        header_name="Variable_Name",
        column_names=["Max"]
    )

    varmean = pldataframe.mean().fill_null("NA").transpose(
        include_header=True,
        header_name="Variable_Name",
        column_names=["Mean"]
    )

    varmedian = pldataframe.median().fill_null("NA").transpose(
        include_header=True,
        header_name="Variable_Name",
        column_names=["Median"]
    )

    varmode = pldataframe.select(pl.all().mode().first()).transpose(
        include_header=True,
        header_name="Variable_Name",
        column_names=["Mode"]
    )

    dfs = [
        vardtypes,
        varnonmissing,
        varmissing,
        vardistinct,
        varmin,
        varmax,
        varmean,
        varmedian,
        varmode
    ]

    cols = pl.concat(pl.Series(df.columns) for df in dfs)
    join_cols = list(cols.filter(cols.is_duplicated()).unique())

    final = dfs[0].join(dfs[1], how="inner", on=join_cols, suffix="")
    # only needed for more than 2 frames
    for df in dfs[2:]:
        final = final.join(df, how="inner", on=join_cols, suffix="")

    final = final.select(*cols.unique(maintain_order=True))

    final = final.with_columns(
        (
            pl.col("No_of_Missing_Values") / pl.col("No_of_Non_Missing_Values") * 100
        ).alias("Missing_%")
    )

    final= final.with_columns(S_No = 1 + pl.int_range(pl.len()))

    final = final.select(
        [
            "S_No",
            "Variable_Name",
            "D_Type",
            "No_of_Non_Missing_Values",
            "No_of_Missing_Values",
            "Missing_%",
            "Distinct_Values",
            "Min",
            "Max",
            "Mean",
            "Median",
            "Mode"
        ]
    )

    del vardtypes, varnonmissing, varmissing, vardistinct, varmin, varmax, varmean, varmedian, varmode, df, dfs, cols, join_cols

    workbookname = f"{find_df_name(pldataframe)}_fact_checks.xlsx"

    final_pdf = final.to_pandas()

    try:
    # Primary method
        return final.write_excel(
        workbook=workbookname,
        worksheet="FCOTD",
        position="B2",
        table_style="Table Style Light 10"
    )
    except Exception as e:
        print(f"Primary Excel write failed: {e}")
    
        try:
            # Fallback method: write without styling
            return final_pdf.to_excel("FCOTD.xlsx",
            index=False
                # Omitting table_style as a fallback
            )
        except Exception as fallback_error:
            print(f"Fallback Excel write also failed: {fallback_error}")

    # Final fallback: save as CSV
            try:
                csv_filename = workbookname.replace(".xlsx", "_fallback.csv")
                final.to_csv(csv_filename, index=False)
                print(f"Saved as CSV instead: {csv_filename}")
                return "Saved as CSV due to Excel write failure"
            except Exception as csv_error:
                print(f"CSV fallback failed: {csv_error}")
                return "All write attempts failed"

#usage example 
##fact_check_of_the_data(polarsdataframe)
