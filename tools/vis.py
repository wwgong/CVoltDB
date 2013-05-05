#!/usr/bin/env python

# This is a visualizer which pulls TPC-C benchmark results from the MySQL
# databases and visualizes them. Four graphs will be generated, latency graph on
# sinigle node and multiple nodes, and throughput graph on single node and
# multiple nodes.
#
# Run it without any arguments to see what arguments are needed.

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))) +
                os.sep + 'tests/scripts/')

import time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from voltdbclient import *

STATS_SERVER = 'volt2'

def COLORS(k):
    return (((k ** 3) % 255) / 255.0,
            ((k * 100) % 255) / 255.0,
            ((k * k) % 255) / 255.0)

MARKERS = ['+', '*', '<', '>', '^', '_',
           'D', 'H', 'd', 'h', 'o', 'p']

def get_stats(hostname, port, days):
    """Get statistics of all runs

    Example return value:
    { u'VoltKV': [ { 'lat95': 21,
                 'lat99': 35,
                 'nodes': 1,
                 'throughput': 104805,
                 'date': datetime object}],
      u'Voter': [ { 'lat95': 20,
                    'lat99': 47,
                    'nodes': 1,
                    'throughput': 66287,
                    'date': datetime object}]}
    """

    conn = FastSerializer(hostname, port)
    proc = VoltProcedure(conn, 'BestOfPeriod',
                         [FastSerializer.VOLTTYPE_SMALLINT])
    resp = proc.call([days])
    conn.close()

    # keyed on app name, value is a list of runs sorted chronologically
    stats = dict()
    run_stat_keys = ['nodes', 'date', 'tps', 'lat95', 'lat99']
    for row in resp.tables[0].tuples:
        app_stats = []
        if row[0] not in stats:
            stats[row[0]] = app_stats
        else:
            app_stats = stats[row[0]]
        run_stats = dict(zip(run_stat_keys, row[1:]))
        app_stats.append(run_stats)

    # sort each one
    for app_stats in stats.itervalues():
        app_stats.sort(key=lambda x: x['date'])

    return stats

class Plot:
    DPI = 100.0

    def __init__(self, title, xlabel, ylabel, filename, w, h):
        self.filename = filename
        self.legends = {}
        w = w == None and 800 or w
        h = h == None and 300 or h
        fig = plt.figure(figsize=(w / self.DPI, h / self.DPI),
                         dpi=self.DPI)
        self.ax = fig.add_subplot(111)
        self.ax.set_title(title)
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        plt.ylabel(ylabel, fontsize=8)
        plt.xlabel(xlabel, fontsize=8)
        fig.autofmt_xdate()

    def plot(self, x, y, color, marker_shape, legend):
        self.ax.plot(x, y, linestyle="-", label=str(legend),
                     marker=marker_shape, markerfacecolor=color, markersize=4)

    def close(self):
        formatter = matplotlib.dates.DateFormatter("%b %d")
        self.ax.xaxis.set_major_formatter(formatter)
        ymin, ymax = plt.ylim()
        plt.ylim((0, ymax * 1.1))
        plt.legend(prop={'size': 10}, loc=0)
        plt.savefig(self.filename, format="png", transparent=False,
                    bbox_inches="tight", pad_inches=0.2)


def plot(title, xlabel, ylabel, filename, width, height, app, data, data_type):
    plot_data = dict()
    for run in data:
        if run['nodes'] not in plot_data:
            plot_data[run['nodes']] = {'time': [], data_type: []}

        datenum = matplotlib.dates.date2num(run['date'])
        plot_data[run['nodes']]['time'].append(datenum)

        if data_type == 'tps':
            value = run['tps']/run['nodes']
        else:
            value = run[data_type]
        plot_data[run['nodes']][data_type].append(value)

    if len(plot_data) == 0:
        return

    i = 0
    pl = Plot(title, xlabel, ylabel, filename, width, height)
    sorted_data = sorted(plot_data.items(), key=lambda x: x[0])
    for k, v in sorted_data:
        pl.plot(v['time'], v[data_type], COLORS(i), MARKERS[i], k)
        i += 3

    pl.close()

def generate_index_file(filenames):
    row = """
      <tr>
        <td>%s</td>
        <td><a href="%s"><img src="%s" width="400" height="200"/></a></td>
        <td><a href="%s"><img src="%s" width="400" height="200"/></a></td>
      </tr>
"""

    full_content = """
<html>
  <head>
    <title>Performance Graphs</title>
  </head>
  <body>
    <table>
%s
    </table>
  </body>
</html>
""" % (''.join([row % (i[0], i[1], i[1], i[2], i[2]) for i in filenames]))
    return full_content

def usage():
    print "Usage:"
    print "\t", sys.argv[0], "output_dir filename_base" \
        " [width] [height]"
    print
    print "\t", "width in pixels"
    print "\t", "height in pixels"

def main():
    if len(sys.argv) < 3:
        usage()
        exit(-1)

    if not os.path.exists(sys.argv[1]):
        print sys.argv[1], "does not exist"
        exit(-1)

    prefix = sys.argv[2]
    path = os.path.join(sys.argv[1], sys.argv[2])
    width = None
    height = None
    if len(sys.argv) >= 4:
        width = int(sys.argv[3])
    if len(sys.argv) >= 5:
        height = int(sys.argv[4])

    stats = get_stats(STATS_SERVER, 21212, 30)

    # Plot single node stats for all apps
    filenames = []              # (appname, latency, throughput)
    for app, data in stats.iteritems():
        app_filename = app.replace(' ', '_')
        latency_filename = '%s-latency-%s.png' % (prefix, app_filename)
        throughput_filename = '%s-throughput-%s.png' % (prefix, app_filename)
        filenames.append((app, latency_filename, throughput_filename))

        plot(app + " latency", "Time", "Latency (ms)",
             path + "-latency-" + app_filename + ".png", width, height, app,
             data, 'lat99')

        plot(app + " throughput", "Time", "Throughput (txns/sec)",
             path + "-throughput-" + app_filename + ".png", width, height, app,
             data, 'tps')

    # generate index file
    index_file = open(path + '-index.html', 'w')
    sorted_filenames = sorted(filenames, key=lambda f: f[0].lower())
    index_file.write(generate_index_file(sorted_filenames))
    index_file.close()

if __name__ == "__main__":
    main()
