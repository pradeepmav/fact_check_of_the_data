import polars as pl
import pandas as pd
import os
from typing import Union, Optional

def find_df_name(df):
    """Find the variable name of a dataframe in the global namespace.
    Args:
    df: The dataframe object to find the name for
    Returns:
    str: The variable name if found, otherwise 'unnamed_dataframe'
    """
    name = [name for name, obj in globals().items() if id(obj) == id(df)]
    return name[0] if name else "unnamed dataframe"

def fact_check_of_the_data(dataframe: pl.DataFrame, output_dir: Optional[str] = None, workbook_name: Optional[str] = None) -> str:
    """Generate comprehensive statistical summaries of a Polars dataframe and export to Excel.
    This function analyzes a Polars dataframe and creates a summary with data types,
    missing values, distinct values, min/max values, and central tendency measures.
    The results are exported to an Excel file with fallback options for CSV.
    Args:
    dataframe: A Polars DataFrame to analyze
    output_dir: Optional directory path for saving output files
    workbook_name: Optional custom name for the output Excel file
    Returns:
    str: Path to the created file or status message
    """
    pldataframe = dataframe # Rename for consistency

    # Create summary dataframes for each statistic
    try:
        # Data types
        vardtypes = pl.DataFrame({
            "Variable_Name": pldataframe.columns,
            "D_Type": [str(dtype) for dtype in pldataframe.dtypes]
        })

        # Non-missing values count
        varnonmissing = pldataframe.count().transpose(
            include_header=True,
            header_name="Variable_Name",
            column_names=["No_of_Non_Missing_Values"],
        )

        # Missing values count
        varmissing = pldataframe.null_count().transpose(
            include_header=True,
            header_name="Variable_Name",
            column_names=["No_of_Missing_Values"],
        )

        # Distinct values count
        vardistinct = pldataframe.select(pl.all().n_unique()).transpose(
            include_header=True,
            header_name="Variable_Name",
            column_names=["Distinct_Values"],
        )
        
        # Identify numeric and non-numeric columns
        numeric_cols = pldataframe.select(pl.selectors.numeric()).columns
        non_numeric_cols = [c for c in pldataframe.columns if c not in numeric_cols]

        # Initialize dataframes to ensure consistent schemas
        varmin = pl.DataFrame({"Variable_Name": [], "Min": []}, schema={"Variable_Name": pl.Utf8, "Min": pl.Utf8})
        varmax = pl.DataFrame({"Variable_Name": [], "Max": []}, schema={"Variable_Name": pl.Utf8, "Max": pl.Utf8})
        varmean = pl.DataFrame({"Variable_Name": [], "Mean": []}, schema={"Variable_Name": pl.Utf8, "Mean": pl.Float64})
        varmedian = pl.DataFrame({"Variable_Name": [], "Median": []}, schema={"Variable_Name": pl.Utf8, "Median": pl.Float64})
        varmode = pl.DataFrame(schema={"Variable_Name": pl.Utf8, "Mode": pl.Utf8})
        
        # Process numeric columns separately and ensure casting
        if numeric_cols:
            varmin_numeric = pldataframe.select(pl.col(c).min().alias(c) for c in numeric_cols).transpose(
                include_header=True,
                header_name="Variable_Name",
                column_names=["Min"],
            ).with_columns(pl.col("Min").cast(pl.Utf8))
            
            varmax_numeric = pldataframe.select(pl.col(c).max().alias(c) for c in numeric_cols).transpose(
                include_header=True,
                header_name="Variable_Name",
                column_names=["Max"],
            ).with_columns(pl.col("Max").cast(pl.Utf8))

            varmean_numeric = pldataframe.select(pl.col(c).mean().alias(c) for c in numeric_cols).transpose(
                include_header=True,
                header_name="Variable_Name",
                column_names=["Mean"],
            )

            varmedian_numeric = pldataframe.select(pl.col(c).median().alias(c) for c in numeric_cols).transpose(
                include_header=True,
                header_name="Variable_Name",
                column_names=["Median"],
            )
            
            # Append to main dataframes
            varmin = pl.concat([varmin, varmin_numeric], how="vertical_relaxed")
            varmax = pl.concat([varmax, varmax_numeric], how="vertical_relaxed")
            varmean = pl.concat([varmean, varmean_numeric], how="vertical_relaxed")
            varmedian = pl.concat([varmedian, varmedian_numeric], how="vertical_relaxed")

        # Process non-numeric columns and fill in nulls for numeric stats
        if non_numeric_cols:
            # Min/Max for string columns (lexicographical comparison)
            varmin_str = pldataframe.select(pl.col(c).min().alias(c) for c in non_numeric_cols).transpose(
                include_header=True,
                header_name="Variable_Name",
                column_names=["Min"],
            ).with_columns(pl.col("Min").cast(pl.Utf8))
            
            varmax_str = pldataframe.select(pl.col(c).max().alias(c) for c in non_numeric_cols).transpose(
                include_header=True,
                header_name="Variable_Name",
                column_names=["Max"],
            ).with_columns(pl.col("Max").cast(pl.Utf8))

            # Create dataframes with null values for mean/median for non-numeric columns
            varmean_str = pl.DataFrame({"Variable_Name": non_numeric_cols, "Mean": [None] * len(non_numeric_cols)})
            varmedian_str = pl.DataFrame({"Variable_Name": non_numeric_cols, "Median": [None] * len(non_numeric_cols)})
            
            # Append to main dataframes
            varmin = pl.concat([varmin, varmin_str], how="vertical")
            varmax = pl.concat([varmax, varmax_str], how="vertical")
            varmean = pl.concat([varmean, varmean_str], how="vertical")
            varmedian = pl.concat([varmedian, varmedian_str], how="vertical")

        # Mode values - completely rewritten to avoid type issues and handle nulls
        modes_list = []
        for c in pldataframe.columns:
            try:
                # Use value_counts to find the most frequent value, handling nulls gracefully
                mode_result = pldataframe.select(pl.col(c).drop_nulls().mode().first().cast(pl.Utf8))
                mode_val = mode_result.item() if not mode_result.is_empty() else None
                modes_list.append({"Variable_Name": c, "Mode": str(mode_val) if mode_val is not None else None})
            except Exception:
                modes_list.append({"Variable_Name": c, "Mode": None})
        varmode = pl.DataFrame(modes_list, schema={"Variable_Name": pl.Utf8, "Mode": pl.Utf8})

        # Combine all summary dataframes using a left join
        dfs = [vardtypes, varnonmissing, varmissing, vardistinct, varmin, varmax, varmean, varmedian, varmode]
        join_cols = ["Variable_Name"]

        # Join the dataframes. `how="left"` is used as the first DF has all column names.
        final = dfs[0]
        for df in dfs[1:]:
            final = final.join(df, how="left", on=join_cols, suffix="_")

        # Calculate missing percentage
        final = final.with_columns(
            ((pl.col("No_of_Missing_Values") / (pl.col("No_of_Non_Missing_Values") + pl.col("No_of_Missing_Values")) * 100)
             .fill_nan(None).round(2)
             .alias("Missing_%"))
        )

        # Add row numbers
        final = final.with_columns(S_No=pl.int_range(1, pl.len() + 1))

        # Reorder columns
        final = final.select(
            ["S_No", "Variable_Name", "D_Type", "No_of_Non_Missing_Values", "No_of_Missing_Values", "Missing_%",
             "Distinct_Values", "Min", "Max", "Mean", "Median", "Mode"]
        )
        
        # Clean up memory
        del vardtypes, varnonmissing, varmissing, vardistinct, varmin, varmax, varmean, varmedian, varmode

        # Determine output file name and path
        if workbook_name is None:
            workbook_name = f"{find_df_name(pldataframe)}_fact_checks.xlsx"
        if output_dir is not None:
            os.makedirs(output_dir, exist_ok=True)
            workbook_path = os.path.join(output_dir, workbook_name)
        else:
            workbook_path = workbook_name

        # Try writing to Excel with Polars
        try:
            final.write_excel(workbook=workbook_path, worksheet="FCOTD", position="B2", table_style="Table Style Light 10")
            return f"Successfully wrote to Excel: {workbook_path}"
        except Exception as e:
            print(f"Primary Excel write failed: {e}")
            # Fallback to pandas Excel writer
            try:
                final_pdf = final.to_pandas()
                final_pdf.to_excel(workbook_path, sheet_name="FCOTD", index=False, startrow=1, startcol=1)
                return f"Successfully wrote to Excel using pandas fallback: {workbook_path}"
            except Exception as fallback_error:
                print(f"Fallback Excel write also failed: {fallback_error}")
                # Final fallback: save as CSV
                try:
                    csv_filename = workbook_path.replace(".xlsx", "_fallback.csv")
                    final.write_csv(csv_filename)
                    print(f"Saved as CSV instead: {csv_filename}")
                    return f"Saved as CSV due to Excel write failure: {csv_filename}"
                except Exception as csv_error:
                    print(f"CSV fallback failed: {csv_error}")
                    return "All write attempts failed"

    except Exception as e:
        print(f"Error during data analysis: {e}")
        return f"Analysis failed: {str(e)}"

# Example usage (uncomment to test)
# df = pl.DataFrame({"A": [1, 2, 3.5, None], "B": ["a", "b", "c", None], "C": [10, 20, 30, 40], "D": ["x", "y", "z", "z"], "E": ["1", "2", "3", "4"]})
# fact_check_of_the_data(df, output_dir="./output", workbook_name="fact_check_report.xlsx")

