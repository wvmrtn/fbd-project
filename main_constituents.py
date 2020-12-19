#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Created on Fri Dec 18 15:32:59 2020.

@author: williammartin
"""

# import standard libraries
import numpy as np
import os
# import third-party libraries
import pandas as pd
import wrds
# import local libraries
from fbd import update_constituents

if __name__ == '__main__':

    # create WRDS object first
    db = wrds.Connection(wrds_username=os.environ['WRDS_USER'],
                         wrds_password=os.environ['WRDS_PASS'])

    start = '2010-01-01'
    end = '2019-12-31'

    dates = pd.date_range(start, end, freq='m')

    # create matrix of constituent matrix, 1 = belongs to Russell 3000.
    # column index correspond to permno (index 0 is permno 0 and so on)
    const_mat = pd.DataFrame(index=dates.strftime('%Y-%m'),
                             columns=range(10000, 99999+1),
                             data=np.nan)
    # 1e5 is the max val of permno

    dates = dates.strftime("%Y-%m-%d")
    for d in dates:
        const_mat = update_constituents(const_mat, db, d)

    # remove stocks that have never been in the russell3000
    const_mat.dropna(how='all', axis=1, inplace=True)
    const_mat.fillna(0, inplace=True)

    const_mat.to_csv('data/russell3000.csv.gz', compression='gzip')
