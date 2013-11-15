import logging
import data_loader
import matplotlib.pyplot as pyplot
import datetime

def get_treasuries_series(series):
    data = data_loader.download_treasuries()
    timeseries = data[series][data_loader.TIMESERIES]
    cleaned = [(date, value) for date, value in timeseries if value != 'ND']
    return zip(*cleaned)
    
def plot_treasuries():
    dates, values = get_treasuries_series('H15/H15/RIFLGFCY10_N.B')

    figure, axes = pyplot.subplots()
    axes.plot_date(dates, values, linestyle='-', marker='')
    figure.autofmt_xdate()
    pyplot.show()

if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')
    plot_treasuries()
