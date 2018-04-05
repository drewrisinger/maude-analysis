import pandas
from typing import Iterable, Any
import logging
import os
import csv
#import sys

from pandas.errors import ParserError


def maude_to_pandas(maude_file_path: str, dtype: Any = "str") -> pandas.DataFrame:
    """
    Reads a FDA MAUDE database file as a Pandas dataframe
    :param maude_file_path: the path to the MAUDE file. Can be relative or absolute.
    :param dtype:
    :return: Pandas dataframe containing all the data
    """
    logging.debug("Reading file {} with dtype={}".format(maude_file_path, dtype))
    try:
        return_frame = pandas.read_csv(maude_file_path, sep='|', encoding='ANSI', dtype=dtype, quoting=csv.QUOTE_NONE)
    except ParserError as e:
        check_bad_csv(maude_file_path)
        raise e

    return return_frame


######################################################
# DEPRECATED. DEPRECATED. DEPRECATED.
######################################################
def add_data_to_mdr(mdr_base: pandas.DataFrame, other_df_files: Iterable[str], base_key: str ="MDR_REPORT_KEY") -> pandas.DataFrame:
    """
    DEPRECATED. DO NOT USE
    Merges other databases into a master record of all information.
    :return: merged Panda Dataframe
    """
    merged_df = mdr_base.copy()
    for df_loc in other_df_files:
        try:
            merged_df = pandas.merge(merged_df, maude_to_pandas(df_loc), how='left', on=base_key)
        except KeyError:
            logging.warning("File {} does not have key {}".format(df_loc, base_key))
    return merged_df
######################################################
# End DEPRECATED function.
######################################################


def compile_maude_database(directory_path: str, reference_directory_path: str, base_file_name: str) -> pandas.DataFrame:
    """
    Compiles several txt MAUDE database files into a single database.
    :param directory_path: Directory where the files are stored
    :param reference_directory_path: Directory where reference files are stored.
    :param base_file_name: The initial file name to construct the database from. Will only use MDR keys from this. Should start with mdr
    :return: DataFrame compiled from all these files
    """
    if not directory_path.endswith('/'):
        raise ValueError("Argument directory path ({}) invalid. Must end with '/'".format(directory_path))
    if not reference_directory_path.endswith('/'):
        raise ValueError("Reference directory ({}) invalid. Must end with '/'".format(reference_directory_path))

    # Constants
    mdr_base_key = "MDR_REPORT_KEY"
    problem_code_file = "deviceproblemcodes.txt"
    problem_code_path = reference_directory_path + problem_code_file
    problem_code_key = "DEVICE_PROBLEM_CODE"
    error_code_header = 'ERR_TYPE'

    # TODO: convert columns to appropriate datatypes
    directory_files = os.listdir(directory_path)
    base_file_path = directory_path + base_file_name
    logging.debug("Constructing database from base file: {}".format(base_file_path))
    entire_dataset = maude_to_pandas(directory_path + base_file_name)

    for file in directory_files:
        file_path = os.path.join(directory_path, file)
        if file_path.endswith(".txt") and not os.path.samefile(file_path, base_file_path) and not file.startswith("mdr"):
            logging.debug("Attempting to add file {} to dataset".format(file))
            try:
                merge_file = maude_to_pandas(file_path)
                entire_dataset.merge(maude_to_pandas(file_path), on=mdr_base_key)
            except KeyError:
                logging.warning("File {} does not have key {}".format(file, mdr_base_key))

    # entire_dataset = add_data_to_mdr(entire_dataset, directory_files)
    problem_codes = maude_to_pandas(problem_code_path)
    problem_codes[error_code_header] = problem_codes[error_code_header].astype('category')
    logging.info("Column headers: {}".format(", ".join(entire_dataset.columns.values)))
    if problem_code_key in entire_dataset.columns.values:
        entire_dataset.merge(problem_codes, on=problem_code_key)
    else:
        logging.error("Dataset doesn't contain any device problem codes. Try a different dataset")
    logging.debug("Final dataset shape: {}\nColumn headers: {}".format(entire_dataset.shape, list(entire_dataset)))

    return entire_dataset


def check_bad_csv(check_file_path: str):
    """
    Function to check if a MAUDE CSV file is badly formatted.
    :param check_file_path: The file to be checked
    :return: None. All data is printed to console
    """
    #csv.field_size_limit(sys.maxsize)
    with open(check_file_path, 'r') as f:
        reader = csv.reader(f, delimiter='|', quoting=csv.QUOTE_NONE)
        first_row = next(reader)
        default_row_length = len(first_row)
        i = 2
        # Read each line, and check if it's the proper length.
        for row in reader:
            if len(row) is not default_row_length:
                logging.error("Error in row {}. Should have {} cols, has {}. "
                              "Row is: \n{}".format(i, len(first_row), len(row), ", ".join(row)))
            i = i+1
