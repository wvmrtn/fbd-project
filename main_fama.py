#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Created on Tue Dec 22 16:50:00 2020.

@author: williammartin
"""

# import standard libraries
# import third-party libraries
# import local libraries
from fbd import download_fama, START, END


if __name__ == '__main__':

    download_fama(START, END)