#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Created on Tue Dec 22 16:50:00 2020.

@author: williammartin
"""

# import standard libraries
# import third-party libraries
import pandas as pd
# import local libraries
from fbd import download_fama, START, END


if __name__ == '__main__':

    fama, mom, st_rev, lt_rev = download_fama(START, END)
    fama = pd.concat([fama, mom, st_rev, lt_rev], axis=1)
    fama.to_csv('data/fama/fama.csv.gz', compression='gzip')
