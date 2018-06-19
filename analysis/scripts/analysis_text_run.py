import pandas as pd
import numpy as np
import src.maude_interface as maude
import logging
import os

logging.basicConfig(level=logging.DEBUG)
logging.debug("Current working dir: " + str(os.getcwd()))
data_folder = "../data/"
reference_folder = "./reference/"

text_data_series = pd.read_pickle(data_folder + "all_text_stemmed.pkl")
logging.debug("Finished loading text data")
text_data_series.describe()

from sklearn.feature_extraction.text import TfidfVectorizer
tfidfv = TfidfVectorizer()
print(text_data_series.values)
tfidfv.fit_transform(text_data_series.str)
