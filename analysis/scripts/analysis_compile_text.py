import pandas as pd
import numpy as np
import src.maude_interface as maude
import logging
import os

logging.basicConfig(level=logging.DEBUG)
logging.debug("Current working dir: " + str(os.getcwd()))
data_folder = "../data/"
reference_folder = "./reference/"

all_text_data = maude.get_all_text_data(data_folder, years=list(range(2012, 2013)))
all_text_data.info()
print(all_text_data.shape)
print(all_text_data.memory_usage())
print(all_text_data.memory_usage().sum())
print(all_text_data.memory_usage(deep=True).sum())

#all_text_data.to_hdf(data_folder + "all_text_data.h5", 'df', format='t', complevel=9, complib='zlib')
#logging.debug("Saved All Text to H5 file")

# preprocessing
just_text_series = all_text_data['FOI_TEXT'].dropna()

del all_text_data

from textblob import TextBlob

def stem_blob(input):
    if input is not np.nan:
        return TextBlob(" ").join((w.stem() for w in TextBlob(input).words.lower()))
    else:
        return input


logging.debug("Starting to apply stemming")
stemmed_text = just_text_series.map(stem_blob)
logging.debug("Finished applying stemming")