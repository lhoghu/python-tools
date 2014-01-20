import datetime
import logging
import config
import data_retrieval
import random

################################################################################

def get_bbg_data_at_cass(index_list, start_date, end_date):
    equ_set = set()

    # Get the time series from the cache
    config.DB = "cache"
    config.SERIALISER = "spickle"

    logging.basicConfig(filename='debug.log', level=logging.DEBUG)
    equity_set = set()
    symbol_to_idx = dict()
    idx_to_symbol = list()
    for index in index_list:
        field = ''
        if index == 'DJI Index':
            field = 'INDX_MWEIGHT'
        else:
            field = 'INDX_MWEIGHT_HIST'

        loader = 'download_bbg_timeseries'
        loader_args = {
                'symbol': index,
                'start': start_date,
                'end': end_date,
                'field': field
            }

        ts = data_retrieval.get_time_series(loader, loader_args)
        for date, equ_tup in ts:
            equ_set.update(equ_tup)
#        if ts is not None:
#            total_ts = loader_args.copy()
#            total_ts['ts'] = ts
#            print "WRITING " + index
#            data_retrieval.write_time_series(total_ts)




    fields = (
        'PX_LAST',
        'PX_OFFICIAL_CLOSE',
        'PX_OPEN',
        'PX_LAST',
        'PX_OPEN',
        'PX_LOW',
        'TOT_RETURN_INDEX_GROSS_DVDS',
        'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS',
        'EQY_DPS',
        'SECURITY_DES',
        'TRAIL_12M_GROSS_MARGIN',
        'ASSET_TURNOVER',
        'BS_SH_OUT',
        'TRAIL_12M_CASH_FROM_OPER',
        'CF_CASH_FROM_OPER',
        'BS_LT_BORROW',
        'LT_DEBT_TO_TOT_ASSET',
        'LIVE_LT_DEBT_TO_TOTAL_ASSETS',
        'CUR_RATIO',
        'CASH_RATIO',
        'QUICK_RATIO',
        'NET_INCOME',
        'CF_CASH_FROM_OPER',
        'RETURN_ON_ASSET',
        'EBIT',
        'EBITDA',
        'TOT_DEBT_TO_TOT_ASSET',
        'TOT_DEBT_TO_COM_EQY',
        'BS_TOT_ASSET',
        'HISTORICAL_MARKET_CAP')

    start_date = datetime.datetime(1960, 1, 1)
    random.seed()

    equ_list = list(equ_set);
    random.shuffle(equ_list)

    for equity in equ_list:
        equity += " Equity"
        for field in fields:
            loader = 'download_bbg_timeseries';
            loader_args = {
                'symbol': equity,
                'start': start_date,
                'end': end_date,
                'field': field
                }

            ts = data_retrieval.get_time_series(loader, loader_args)


#            if ts is not None:
#                print "WRITING " + equity
#                total_ts = loader_args.copy()
#                total_ts['ts'] = ts
#                config.DB = data_retrieval.MONGO_DB
#                data_retrieval.write_time_series(total_ts)

################################################################################

if __name__ == '__main__':
    index_list = ('SPX Index',)
    # index_list = ('SPX Index', 'DJI Index', 'RTY Index')
    # index_list = ('SPX Index', 'DJI Index', 'RTY Index', 'RAY Index')
    t = datetime.datetime(2014, 1, 4)
    today_date = datetime.datetime(t.year, t.month, t.day)
    start_date = datetime.datetime(1990, 1, 1)
    end_date = today_date - datetime.timedelta(1)
    get_bbg_data_at_cass(index_list, start_date, end_date)