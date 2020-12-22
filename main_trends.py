#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Created on Mon Dec 21 19:30:28 2020.

@author: williammartin
"""

# import standard libraries
import time
import numpy as np
# import third-party libraries
import pandas as pd
from pytrends.request import TrendReq
# import local libraries
from fbd import START, END

if __name__ == '__main__':

    # get stock info
    info = pd.read_csv('data/permno_info.csv.gz', compression='gzip',
                       index_col=0)
    info['permno'] = info['permno'].astype(int)

    # download constituents
    const_mat = pd.read_csv('data/russell3000.csv.gz', compression='gzip',
                            index_col=0)

    dates_start = pd.date_range(START, END, freq='MS')
    dates_end = pd.date_range(START, END, freq='M')

    # create trend requester
    pytrends = TrendReq(timeout=(10, 25))

    for i, ds in enumerate(dates_start):
        di = ds.strftime('%Y-%m-%d')[:7]
        de = dates_end[i]
        permnos = const_mat.loc[di][const_mat.loc[di] == 1].index.values

        # create trends df
        trends_df = pd.DataFrame(index=pd.date_range(ds, de),
                                 columns=permnos, data=np.nan)

        for p in permnos:
            now = time.time()
            stock = info[info['permno'] == int(p)]
            kw_list = list(stock['comnam'])
            kw_list.extend(list(stock['ticker']))
            kw_list = list(set(kw_list))[:5]  # google limit of 5 keywords

            data = pytrends.get_historical_interest(
                    kw_list,
                    year_start=ds.year, month_start=ds.month, day_start=ds.day,
                    year_end=de.year, month_end=de.month, day_end=de.day,
                    sleep=0,
                    # gprop='news',
                    # geo='US',
                    # cat=784
                    )

            print('{} - {}'.format(p, time.time()-now))

            if not data.empty:
                trend = data.groupby(data.index.day).sum().sum(axis=1)

                trends_df.at[:, p] = trend.values
            else:
                trends_df.at[:, p] = 0

        break
