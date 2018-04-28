from __future__ import print_function, unicode_literals
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators
from alpha_vantage.sectorperformance import SectorPerformances
from alpha_vantage.cryptocurrencies import CryptoCurrencies
import requests
import logging

log = logging.getLogger(__name__)

try:
    import simplejson as json
except ImportError:
    import json


VANTAGE_API = """
Welcome to Alpha Vantage! Your dedicated access key is: ZAKJIJD5JMHQ1MW1. 
Please record this API key for lifetime access to Alpha Vantage
"""


class TechnicalAnalysis(object):
    """
    base_url = "https://www.alphavantage.co/query"
    args = {'function': 'TIME_SERIES_INTRADAY', 'symbol': 'MSFT', 'interval': '15min', 'apikey': 'ZAKJIJD5JMHQ1MW1'}
    res = requests.get(base_url, params=args)
    a = AvBaseApi(mykey)
    a[function] = TIME_SERIES_INTRADAY
    a[symbol] = MSFT
    a[interval] = 15min
    a.call()
    """

    def __init__(self, apikey):
        self.apikey = apikey
        self.params = dict()

    def __setitem__(self, key, value):
        self.params[key] = value

    def __getitem__(self, key):
        return self.params[key]

    def __delitem__(self, key):
        del self.params[key]

    def __missing__(self, key):
        log.info("Parameter not set yet")
        return None

    def __call__(self, *args, **kwargs):
        pass

    @property
    def report(self):
        return None
