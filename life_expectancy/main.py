"""

"""
import logging
import argparse

from life_expectancy import config, cleaning


# pylint: disable=W0102
def main(region: list[str] = ['PT']) -> None:
    """
    Perform an ETL on the EU life expectancy file (according to assignment 2)
    """
    logging.info('initialized main')
    data_cleaner = cleaning.DataCleaner(config.EU_LIFE_EXPECTANCY_FN)
    data_cleaner.extract()
    data_cleaner.transform(
        id_vars=['unit', 'sex', 'age', 'geo'],
        region=region,
        rename_cols={'geo': 'region'}
    )
    data_cleaner.load(config.PT_LIFE_EXPECTANCY_FN)
    logging.info('finished running main')


# pylint: disable=C0116
def parse_cli_args() -> dict:
    description = """
        Reads a file with lifetime expectancy per year, unit, sex, age, geo, unpivots it and saves to a new files.
        """
    logging.info("start parsing args")
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-r",
        "--region",
        type=str,
        default=['PT'],
        required=False,
        nargs='+',
        help="list of countries of the export in the final file"
    )
    return parser.parse_args()


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format='{asctime} - {filename} - {levelname}: {message}',
        datefmt="%Y-%m-%d %H:%M:%S",
        style="{"
    )
    args = parse_cli_args()
    main(args.region)
