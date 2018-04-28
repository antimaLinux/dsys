from __future__ import print_function, unicode_literals
from sqlitedict import SqliteDict
from functools import wraps
import requests
import pandas
import logging
import time
from pprint import pprint

try:
    import simplejson as json
except ImportError:
    import json


log = logging.getLogger(__name__)


class InvalidParameters(Exception):
    pass

supported_identifiers = (
    'ID_ISIN', 'ID_BB_UNIQUE', 'ID_SEDOL', 'ID_COMMON',
    'ID_WERTPAPIER', 'ID_CUSIP', 'ID_CINS', 'ID_BB', 'ID_ITALY',
    'ID_EXCH_SYMBOL', 'ID_FULL_EXCHANGE_SYMBOL', 'COMPOSITE_ID_BB_GLOBAL',
    'ID_BB_GLOBAL_SHARE_CLASS_LEVEL', 'ID_BB_GLOBAL',
    'ID_BB_SEC_NUM_DES', 'TICKER', 'ID_CUSIP_8_CHR', 'OCC_SYMBOL',
    'UNIQUE_ID_FUT_OPT', 'OPRA_SYMBOL', 'TRADING_SYSTEM_IDENTIFIER'
)


class BaseRequestParams(dict):

    args_mapping = {
        'idType': (str,),
        'idValue': (str, int),
        'exchCode': (str,),
        'micCode': (str,),
        'currency': (str,),
        'marketSecDes': (str,)
    }

    required = ('idType', 'idValue')

    def __init__(self, *args, **kwargs):
        super(BaseRequestParams, self).__init__(*args, **kwargs)

        if not self.valid:
            raise InvalidParameters("Not valid requests parameters")

    def check_type(self, key):
        return any([isinstance(self[key], mapping) for mapping in self.args_mapping[key]])

    @property
    def requirements_satisfied(self):
        return all([p in self for p in self.required])

    @property
    def types_correct(self):
        return all([self.check_type(a) for a in self.keys()])

    @property
    def idtype_verified(self):
        return self['idType'] in supported_identifiers

    @property
    def valid(self):
        try:
            if self.requirements_satisfied and self.idtype_verified:
                return True
            else:
                return False
        except KeyError as e:
            log.error("Unknown parameter in request. {}".format(e))
            return False

    @property
    def params_key(self):
        return '_'.join([self[k] for k in self.keys() if self[k]])


class ApiResponse(object):
    def __init__(self, _response):
        self.url = _response.url
        self.time_taken = _response.elapsed
        self.encoding = _response.encoding
        self.headers = _response.headers
        self.status = _response.status_code
        self.data = json.loads(_response.text)[0]['data'] if self.status == 200 else []

    def __repr__(self):
        return "< Response status={} data={}".format(self.status, self.data)


class ApiRequest(object):
    def __init__(self, base_url, apikey=None, observers=()):
        self.base_url = base_url
        self.apikey = apikey
        self.observers = observers

    def call(self, request_params, format_resp='json'):
        params = [BaseRequestParams(**params) for params in request_params]
        headers = {}

        if self.apikey:
            headers = {'X-OPENFIGI-APIKEY': self.apikey}

        resp = ApiResponse(requests.post(self.base_url, json=params, headers=headers))
        if format_resp == 'pandas':
            resp.data = pandas.DataFrame(resp.data)

        for observer in self.observers:
            for param in params:
                observer.notify(param.params_key, resp)

        return resp


class BaseObserver(object):
    def __init__(self, name, output_vector):
        self.name = name
        self.vector = output_vector

    def notify(self, key, data):
        raise NotImplementedError("This method must be implemented")


class DbObserver(BaseObserver):
    def __init__(self, *args, **kwargs):
        super(DbObserver, self).__init__(*args, **kwargs)

    def notify(self, key, data):
        _data = self.convert_date(data).T.to_dict().values() if isinstance(data, pandas.DataFrame) else data
        with SqliteDict(self.vector, tablename='cached_responses') as dbdict:
            dbdict[key] = _data
            dbdict.commit()

    @staticmethod
    def convert_date(df, date_format='%Y-%m-%d'):
        cols = df.loc[:, df.dtypes == 'datetime64[ns]'].columns
        for col in cols:
            df[col] = df[col].map(lambda x: x.strftime(date_format) if pandas.notnull(x) else '')

        return df


class ApiUtils(object):
    @classmethod
    def rate_limited(cls, func):

        last_time_called = [0.]

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            min_interval = 1.0 / float(self.rate_limit_cap)
            elapsed = time.clock() - last_time_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            ret = func(*args, **kwargs)
            last_time_called[0] = time.clock()
            return ret

        return wrapper


class OpenFigiApi(object):

    def __init__(self, db_path='/tmp/data.db', apikey=None):
        self.rate_limit_cap = 25000 if apikey else 25
        self.db_path = db_path
        self.base_url = 'https://api.openfigi.com/v1/mapping'
        self.api_interface = ApiRequest(self.base_url, apikey, observers=[
            DbObserver(name='DbObserver', output_vector=self.db_path)
        ])

    def fetch_data(self, *args, **kwargs):

        params = self.compose_request_params(*args, **kwargs)

        output_format = self.get_output_format(*args, **kwargs)

        with SqliteDict(self.db_path, tablename='cached_responses') as dbdict:
            result = dbdict.get(self.compose_db_key(params), None)

        if result:
            return result
        else:
            return self.api_call(self, params, output_format=output_format)

    def fetch_data_multi(self, bulk_requests):
        pass

    @staticmethod
    def compose_db_key(params):
        return '_'.join([params[k] for k in params.keys() if params[k]])

    @staticmethod
    def compose_request_params(*_, **kwargs):
        return dict(
            idType=kwargs.get(str('id_type'), None),
            idValue=kwargs.get(str('id_value'), None),
            exchCode=kwargs.get(str('exch_code'), None),
            micCode=kwargs.get(str('mic_code'), None),
            currency=kwargs.get(str('curr'), None),
            marketSecDes=kwargs.get(str('market_sec_des'), None)
        )

    @staticmethod
    def get_output_format(*_, **kwargs):
        return kwargs.get(str('output_format'), 'json')

    @ApiUtils.rate_limited
    def api_call(self, params, output_format):
        return self.api_interface.call([{k: v for k, v in params.items() if v is not None}],
                                       format_resp=output_format)


if __name__ == '__main__':
    api = OpenFigiApi()
    response = api.fetch_data(id_type="ID_ISIN", id_value="US4642872265", output_format='json')
    pprint(response.data[0])
    response = api.fetch_data(id_type="TICKER", id_value="AGG", currency="USD", output_format='json')
    pprint(response.data)
