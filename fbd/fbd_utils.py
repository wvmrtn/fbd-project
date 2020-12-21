#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Created on Fri Dec 11 16:40:54 2020.

@author: williammartin
"""

# import standard libraries
from datetime import timedelta
# import third-party libraries
import pandas as pd
# import local libraries


def update_constituents(const_mat, db, date, n=3000):

    # create empty dataframe
    data = pd.DataFrame()

    # get data of last day of moment, try again if we are a weekend or other
    while data.empty:
        data = \
            db.raw_sql(
                "select permno, shrout, prc from crsp.dsf where "
                f"date >= '{date}' and date <= '{date}'")

        # go back one day
        date = pd.to_datetime(date) - timedelta(days=1)
        date = date.strftime('%Y-%m-%d')

    data['mcap'] = data['shrout'].abs()*data['prc'].abs()
    data.drop(columns=['shrout', 'prc'], inplace=True)

    # retain 3000 biggest mcap
    data = data.nlargest(n, 'mcap')

    # turn permno to int
    data['permno'] = data['permno'].astype(int)

    # add ones to const mat
    const_mat.at[date[:-3], data['permno'].values] = 1

    return const_mat


def download_returns(db, start, end, permnos):

    # get stock returns
    data = \
        db.raw_sql(
            f"select permno, date, ret from crsp.dsf where permno in "
            f"({', '.join(permnos)}) and date >= '{start}' and date "
            f"<= '{end}'")

    data = data.set_index('date')
    
    # clean into dataframe
    X = pd.DataFrame()
    for per, df in data.groupby('permno'):
        df = df.drop(columns='permno')
        df.columns = [int(per)]
        X = pd.concat([X, df], axis=1, ignore_index=False)

    return X
        
    return data

if __name__ == '__main__':
    pass
