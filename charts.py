import matplotlib.pyplot

################################################################################

colour_cycle = ['red', 'green', 'blue', 'yellow']

################################################################################

def line(dates, series):
    """
    Plot series as a line chart
    series should be an array of tuples where each tuple is of the form
    (title, [values])
    """
    # matplotlib.pyplot.gca().set_color_cycle(colour_cycle)

    def plot_and_return_title(plot_title, plot_data):
        matplotlib.pyplot.plot(dates, plot_data)
        return plot_title

    legend = [plot_and_return_title(title, data) for title, data in series]
    matplotlib.pyplot.legend(legend, loc='upper left')

    matplotlib.pyplot.show()

################################################################################

def scatter(series):
    """
    Scatter plot x vs y
    series should be an array of tuples where each tuple is of the form
    (title, [values])
    """

    if len(series) != 2:
        raise ValueError("""Can only plot 2 series in a scatter plot.
        Received {0}""".format(str(len(series))))

    if len(series[0]) != 2:
        raise ValueError("""First element of series array of length {0}.
        It should be of a tuple of length 2""".format(str(len(series[0]))))

    if len(series[1]) != 2:
        raise ValueError("""Second element of series array of length {0}.
        It should be of a tuple of length 2""".format(str(len(series[1]))))

    matplotlib.pyplot.scatter(series[0][1], series[1][1], marker='.')
    matplotlib.pyplot.xlabel(series[0][0])
    matplotlib.pyplot.ylabel(series[1][0])
    matplotlib.pyplot.show()

################################################################################
