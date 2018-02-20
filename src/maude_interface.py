import pandas


def maude_to_pandas(maude_file_path):
    """
    Reads a FDA MAUDE database file as a Pandas dataframe
    :param maude_file_path: the path to the MAUDE file. Can be relative or absolute.
    :return: Pandas dataframe containing all the data
    """
    return pandas.read_csv(maude_file_path, sep='|', encoding='ANSI')

def convert_problem_code_to_description(devproblem_db):
    #todo
    pass
