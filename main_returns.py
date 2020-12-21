#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Created on Sat Dec 19 23:39:16 2020.

@author: williammartin
"""

# import standard libraries
import os
# import third-party libraries
import pandas as pd
import wrds
# import local libraries
from fbd import download_returns
from fbd import START, END

if __name__ == '__main__':

    # create WRDS object first
    db = wrds.Connection(wrds_username=os.environ['WRDS_USER'],
                         wrds_password=os.environ['WRDS_PASS'])

    # download russell 3k
    const_mat = pd.read_csv('data/russell3000.csv.gz', compression='gzip',
                            index_col=0)

    dates_start = pd.date_range(START, END, freq='MS')
    dates_start = dates_start.strftime("%Y-%m-%d")
    dates_end = pd.date_range(START, END, freq='M')
    dates_end = dates_end.strftime("%Y-%m-%d")

    for i, d in enumerate(dates_start):
        di = d[:7]
        permnos = const_mat.loc[di][const_mat.loc[di] == 1].index.values
        returns = download_returns(db, d, dates_end[i], permnos)

        # for parquet, columns and inx needs to be strings
        returns.columns = returns.columns.astype(str)
        returns.index = returns.index.astype(str)

        # save to parquet
        filename = f'data/returns/raw/{di}.parquet'
        if not os.path.exists(filename):
            returns.to_parquet(filename)
