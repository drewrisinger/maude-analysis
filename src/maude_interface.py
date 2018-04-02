import pandas
from typing import Iterable
import logging
import os


def maude_to_pandas(maude_file_path: str) -> pandas.DataFrame:
    """
    Reads a FDA MAUDE database file as a Pandas dataframe
    :param maude_file_path: the path to the MAUDE file. Can be relative or absolute.
    :return: Pandas dataframe containing all the data
    """
    logging.debug("Reading file {}".format(maude_file_path))
    return pandas.read_csv(maude_file_path, sep='|', encoding='ANSI')


def convert_problem_code_to_description(devproblem_db):
    #todo
    pass


def add_data_to_mdr(mdr_base: pandas.DataFrame, other_df_files: Iterable[str], base_key: str ="MDR_REPORT_KEY") -> pandas.DataFrame:
    """
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


def compile_maude_database(directory_path: str, base_file_name: str) -> pandas.DataFrame:
    """
    Compiles several txt MAUDE database files into a single database.
    :param directory_path: Directory where the files are stored
    :param base_file_name: The initial file name to construct the database from. Will only use MDR keys from this
    :return: DataFrame compiled from all these files
    """
    if not directory_path.endswith('/'):
        raise ValueError("Argument directory path ({}) invalid. Must end with '/'".format(directory_path))

    mdr_base_key = "MDR_REPORT_KEY"
    directory_files = os.listdir(directory_path)
    base_file_path = directory_path + base_file_name
    logging.debug("Constructing database from base file: {}".format(base_file_path))
    entire_dataset = maude_to_pandas(directory_path + base_file_name)

    for file in directory_files:
        if file.endswith(".txt"):
            logging.debug("Attempting to add file {} to dataset".format(file))
            try:
                entire_dataset.merge(entire_dataset, maude_to_pandas(os.path.join(directory_path,file)),
                                     how='left', on=mdr_base_key)
            except KeyError:
                logging.warning("File {} does not have key {}".format(file, mdr_base_key))

    # entire_dataset = add_data_to_mdr(entire_dataset, directory_files)
    entire_dataset.merge(maude_to_pandas("./reference/deviceproblemcodes.txt"), on='DEVICE_PROBLEM_CODE')
    logging.debug("Final dataset shape: {}\nColumn headers: {}".format(entire_dataset.shape, list(entire_dataset)))

    return entire_dataset
