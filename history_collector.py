import os
import time
import requests
import numpy as np
import pandas as pd
import datetime as dt

import investpy
import yfinance as yf
import financedatabase as fd
import pandas_datareader.data as web
from bs4 import BeautifulSoup as bs

from utils import *
from constants import *
from preprocessor import *

pd.options.mode.chained_assignment = None