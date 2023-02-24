"""
Cleaning object that performs an entire ETL on the selected file.
"""
import logging

from typing import Optional
from collections.abc import Iterable
import pandas as pd

from life_expectancy.types import StrDict


class DataCleaner:
    """
    Object performs an ETL on a file with the following characteristics:
        1) a single index column containing a concatenation of coman separated components
            ex: unit,sex,age,region
        2) numerical information for each year column
        3) have a region indication in the index
    The ETL reshapes the data to be in a format where the indexes are in different columns
    and the all years are in a single column along with the values.
    """
    def __init__(self, input_fn: str):
        self.input_fn: str = input_fn
        self.raw_df: Optional[pd.DataFrame] = None
        self.transformed_df: Optional[pd.DataFrame] = None

    # pylint: disable=C0116
    def extract(self) -> None:
        """
        Loads the initialized csv file
        """
        logging.info('start extract')
        self.raw_df = pd.read_csv(self.input_fn, sep="\t")

    def _transform_validations(self):
        assert self.raw_df is not None, "extract the data first"

    def _unpivot(self, id_vars: Iterable) -> None:
        expanded_index = self.raw_df.iloc[:, 0].str.split(',', expand=True)
        index_cols = self.raw_df.columns[0].replace("\\", ",").split(",")[:-1]
        expanded_index.columns = index_cols
        year_values = self.raw_df.iloc[:, 1:]
        year_values.columns = year_values.columns.str.strip()
        years = year_values.columns
        expanded_df = pd.concat([expanded_index, year_values], axis=1)
        self.transformed_df = pd.melt(expanded_df, id_vars, years, var_name='year')

    def _rename(self, rename_cols: Optional[StrDict] = None):
        if rename_cols:
            self.transformed_df.rename(columns=rename_cols, inplace=True)

    def _filter(self, region: Optional[list[str]] = None) -> None:
        if not region:
            return
        region = [r.upper() for r in region]
        region_col = self.transformed_df.region.str.upper()
        mask = region_col.isin(region)
        if not mask.any():
            logging.warning("the selected region doesn't match any region - IGNORED")
            return
        self.transformed_df = self.transformed_df[mask]

    def _reformat(self) -> None:
        value = self.transformed_df.value.str.extract(r'(\d+.\d)')
        self.transformed_df["value"] = value.astype(float)
        self.transformed_df = self.transformed_df[self.transformed_df.value.notnull()]

    def transform(
            self,
            id_vars: Iterable,
            region: Optional[list[str]] = None,
            rename_cols: Optional[StrDict] = None) -> None:
        """
        It unpivots the data, cleans the data and filters to the selected regions
        and optionally also renames the columns
        :param id_vars: the variables that be place in the line of the unpivoted table
        :param region: list of regions that to be kept in the final data frame
        :param rename_cols: dictionary mapping the old column name to the new
        """
        logging.info('start transform')
        self._transform_validations()
        self._unpivot(id_vars)
        self._rename(rename_cols)
        self._filter(region)
        self._reformat()

    def _load_validations(self):
        assert self.transformed_df is not None, "transform the data first"

    def load(self, output_fn: str) -> None:
        """
        Write output to desired file
        :param output_fn: destination file name
        """
        logging.info('start loading')
        self._load_validations()
        logging.info(f'writing {len(self.transformed_df)} to file')
        self.transformed_df.to_csv(output_fn, index=False)
