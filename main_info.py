#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Created on Mon Dec 21 18:49:51 2020.

@author: williammartin
"""

# import standard libraries
import os
# import third-party libraries
import pandas as pd
import wrds
# import local libraries
from fbd import download_info

if __name__ == '__main__':

    # create WRDS object first
    db = wrds.Connection(wrds_username=os.environ['WRDS_USER'],
                         wrds_password=os.environ['WRDS_PASS'])

    # download russell 3k
    const_mat = pd.read_csv('data/russell3000.csv.gz', compression='gzip',
                            index_col=0)

    # get some info
    permnos = const_mat.columns.values
    info = download_info(db, permnos)

    # save
    info.to_csv('data/permno_info.csv.gz', compression='gzip')
