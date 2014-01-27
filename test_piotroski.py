import logging
import logging.handlers
import datetime
import data_retrieval
import sys
import config

# A. Profitability Signals
# 1 Net Income - Score 1 if there is positive net income in the current year.
# 2 Operating Cash Flow - Score 1 if there is positive cashflow from operations in the current year.
# 3.     Return on Assets - Score 1 if the ROA is higher in the current period compared to  the previous year.
# 4.     Quality of Earnings - Score 1 if the cash flow from operations exceeds net income before extraordinary items.
#
# B. Leverage, Liquidity and Source of Funds
# 5.     Decrease in leverage - Score 1 if there is a lower ratio of long term debt to in the current period compared value in the previous year .
# 6.     Increase in liquidity - Score 1 if there is a higher current ratio this year compared to the previous year.
# 7.     Absence of Dilution - Score 1 if the Firm did not issue new shares/equity in the preceding year.
#
# C. Operating Efficiency
# 8.     Score 1 if there is a higher gross margin compared to the previous year.
# 9.     Asset Turnover - Score 1 if there is a higher asset turnover ratio year on year (as a measure of productivity).
# - See more at: http://www.stockopedia.com/content/the-piotroski-f-score-a-fundamental-screen-for-value-stocks-55711/#sthash.0yc4bzoc.dpuf

################################################################################
import test_fetchequitybbg


def get_equity_ts(symbol, field, start_date, end_date):
    loader = 'download_bbg_timeseries';
    loader_args = {
        'symbol': symbol + ' Equity',
        'start': start_date,
        'end': end_date,
        'field': field
    }
    equ_ts = data_retrieval.get_from_cache(loader, loader_args)
    return equ_ts

################################################################################

def generate_piotrosky_coeffs(equ_list, start_date, end_date):
    """
    Generate the Piotrosky coefficients
    @param equ_ts: dictionary<date, tuple> contain equity composition per date
    """
    for equ in equ_list:
        logging.info('** Working on <' + equ + '>')
        net_income_ts = get_equity_ts(equ, 'NET_INCOME', start_date, end_date)
        oper_cf_ts = get_equity_ts(equ, 'CF_CASH_FROM_OPER', start_date, end_date)
        roa_ts = get_equity_ts(equ, 'RETURN_ON_ASSET', start_date, end_date)
#        net_inc_before_extra_items_ts =
        lt_debt_to_assets_ratio_ts = get_equity_ts(equ, 'LT_DEBT_TO_TOT_ASSET', start_date, end_date)
        current_ratio_ts = get_equity_ts(equ, 'CUR_RATIO', start_date, end_date)
#        new_equ_issue_ts =
        gross_margin_ts = get_equity_ts(equ, 'TRAIL_12M_GROSS_MARGIN', start_date, end_date)
        asset_turnover_ts = get_equity_ts(equ, 'ASSET_TURNOVER', start_date, end_date)
        valid_equ = True
        if not net_income_ts:
            valid_equ = False
            logging.info('  missing NET_INCOME <' + equ + '>')

        if not oper_cf_ts:
            valid_equ = False
            logging.info('  missing CF_CASH_FROM_OPER <' + equ + '>')

        if not roa_ts:
            valid_equ = False
            logging.info('  missing RETURN_ON_ASSET <' + equ + '>')

        if not lt_debt_to_assets_ratio_ts:
            valid_equ = False
            logging.info('  missing LT_DEBT_TO_TOT_ASSET <' + equ + '>')

        if not current_ratio_ts:
            valid_equ = False
            logging.info('  missing CUR_RATIO <' + equ + '>')

        if not gross_margin_ts:
            valid_equ = False
            logging.info('  missing TRAIL_12M_GROSS_MARGIN <' + equ + '>')

        if not asset_turnover_ts:
            valid_equ = False
            logging.info('  missing ASSET_TURNOVER <' + equ + '>')

        if valid_equ:
            logging.info("**valid equity " + str(equ))


################################################################################
def generate_us_equity_universe(idx_list, start_date, end_date):
    equity_list_per_date = dict()

    for index in idx_list:
        field = ''
        if index == 'DJI Index':
            field = 'INDX_MWEIGHT'
        else:
            field = 'INDX_MWEIGHT_HIST'

        loader = 'download_bbg_timeseries';
        loader_args = {
                'symbol': index,
                'start': start_date,
                'end': end_date,
                'field': field
            }

        index_composition_ts = data_retrieval.get_time_series(loader, loader_args)

        if index_composition_ts:
            for date, equ_tup in index_composition_ts:
                if not date in equity_list_per_date:
                    equity_list_per_date[date] = set(equ_tup[1:])
                equity_list_per_date[date].update(equ_tup[1:])


    return equity_list_per_date

################################################################################


def generate_piotroski(index_list):
    t = datetime.datetime(2014, 1, 4)
    today_date = datetime.datetime(t.year, t.month, t.day)
    idx_start_date = datetime.datetime(1990, 1, 1)
    idx_end_date = today_date - datetime.timedelta(1)

    equ_list = test_fetchequitybbg.get_bbg_indices(index_list, idx_start_date, idx_end_date)

    equ_start_date = datetime.datetime(1960, 1, 1)
    equ_end_date = today_date - datetime.timedelta(1)
    piotr_ts = generate_piotrosky_coeffs(equ_list, equ_start_date, equ_end_date)

################################################################################

if __name__ == '__main__':
    # Set up a specific logger with our desired output level
    #my_logger = logging.getLogger('MyLogger')
    #my_logger.setLevel(logging.DEBUG)

    # Add the log message handler to the logger
    logging.basicConfig(level=logging.DEBUG)
    log_filename = './%s-piotrosky-log.txt' % datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d %H%M%S")
    handler = logging.handlers.RotatingFileHandler(log_filename, maxBytes=1000000, backupCount=50)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)

    config.DB = "cache"
    config.SERIALISER = "spickle"
    #index_list = ('SPX Index', 'RIY Index', 'RTY Index')
    #index_list = ('RIY Index',)
    index_list = ('SPX Index',)
    try:
        generate_piotroski(index_list)
    except Exception as e:
        logging.exception("Uncaught failure during execution")
        raise