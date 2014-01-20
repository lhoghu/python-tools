import logging
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

index_list = ('SPX Index',)
# index_list = ('SPX Index', 'DJI Index', 'RTY Index')
# index_list = ('SPX Index', 'DJI Index', 'RTY Index', 'RAY Index')
t = datetime.datetime(2014, 1, 4)
today_date = datetime.datetime(t.year, t.month, t.day)
start_date = datetime.datetime(1990, 1, 1)
end_date = today_date - datetime.timedelta(1)


################################################################################

def get_equity_ts(symbol, field):
    loader = 'download_bbg_timeseries';
    loader_args = {
        'symbol': symbol,
        'start': start_date,
        'end': end_date,
        'field': field
    }
    equ_ts = data_retrieval.get_time_series(loader, loader_args)
    return equ_ts

################################################################################

def generate_piotrosky_coeffs(equ_ts):
    """
    Generate the Piotrosky coefficients
    @param equ_ts: dictionary<date, tuple> contain equity composition per date
    """

    all_equ = set()
    for date, equ_tup in equ_ts.iteritems():
        all_equ.update(equ_tup)

    for equ in all_equ:
        net_income_ts = get_equity_ts(equ, 'NET_INCOME')
        oper_cf_ts = get_equity_ts(equ, 'CF_CASH_FROM_OPER')
        roa_ts = get_equity_ts(equ, 'RETURN_ON_ASSET')
#        net_inc_before_extra_items_ts =
        lt_debt_to_assets_ratio_ts = get_equity_ts(equ, 'LT_DEBT_TO_TOT_ASSET')
        current_ratio_ts = get_equity_ts(equ, 'CUR_RATIO')
#        new_equ_issue_ts =
        gross_margin_ts = get_equity_ts(equ, 'TRAIL_12M_GROSS_MARGIN')
        asset_turnover_ts = get_equity_ts(equ, 'ASSET_TURNOVER')
        if net_income_ts and oper_cf_ts and roa_ts and lt_debt_to_assets_ratio_ts and current_ratio_ts and gross_margin_ts and asset_turnover_ts:
            print "valid equity " + str(equ)
        else:
            print "invalid equity " + str(equ)


################################################################################
def generate_us_equity_universe():
    logging.basicConfig(filename='debug.log', level=logging.DEBUG)
    t = datetime.datetime(2014, 1, 4)
    today_date = datetime.datetime(t.year, t.month, t.day)
    start_date = datetime.datetime(1990, 1, 1)
    end_date = today_date - datetime.timedelta(1)

    equity_list_per_date = dict()

    for index in index_list:
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


def generate_piotroski():
    equ_ts = generate_us_equity_universe()
    piotr_ts = generate_piotrosky_coeffs(equ_ts)

################################################################################

if __name__ == '__main__':
    generate_piotroski()