from typing import List
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(style="darkgrid")

import itertools
import json

# Seaborn tutorial
# https://seaborn.pydata.org/tutorial/relational.html#relational-tutorial

# Pandas melt & data tidy
# https://hackersandslackers.com/reshaping-pandas-dataframes/
# https://tomaugspurger.github.io/modern-5-tidy.html


def plot_time_series_cum(data: List[dict]):
    '''
    :param data: A list for each server, then a 2D array of ints as the data
    :return:
    '''
    # In the future, we should store the num_ops, replication_degree, key_range alongside the datapoints as additional values.
    # Columns: time, value, num_ops,
    # Where value is a column with all of put_commit, put_abort, get, get_negack all there.
    # Do something else for measuring throughput...

    # ['client_threads' 'get' 'get_cum' 'get_negack' 'get_negack_cum'
    #  'key_range' 'latency' 'latency_cum' 'num_ops' 'ops' 'ops_cum' 'put_abort'
    #  'put_abort_cum' 'put_commit' 'put_commit_cum' 'replication_degree'
    #  'server_ip' 'throughput' 'throughput_cum' 'time_elapsed_ms'
    #  'time_elapsed_ms_cum']

    # static vars:
    # ['time_elapsed_ms'

    df = pd.DataFrame(data)
    print(df.columns.values)

    # https://stackoverflow.com/questions/13851535/delete-rows-from-a-pandas-dataframe-based-on-a-conditional-expression-involving
    # Filter out replication_degree = 2, key_range = 10, num_ops = 100 entries
    df = df[(df['replication_degree'] != 2) | (df['key_range'] != 10) | (df['num_ops'] != 100)]

    # tidy = pd.melt(df.reset_index(), id_vars=['time_elapsed_ms_cum'], value_vars=['put_commit', 'get', 'put_abort', 'get_negack'], var_name='cmd', value_name='count')

    # THROUGHPUT PLOTS
    tidy_throughput = pd.melt(df.reset_index(),
                              id_vars=['time_elapsed_ms_cum', 'server_ip', 'key_range', 'replication_degree'],
                              value_vars=['throughput', 'throughput_cum'],
                              var_name='type',
                              value_name='throughput_combined')
    # Point plot of throughput (I like this one the best)
    sns.relplot(x='time_elapsed_ms_cum', y='throughput_combined', hue='type',
                col='key_range', row='replication_degree', data=tidy_throughput)
    # # Line plot of throughput
    # sns.relplot(x='time_elapsed_ms_cum', y='throughput_combined', hue='type',
    #             kind='line', data=tidy_throughput)
    # # Multiline plot of throughput
    # sns.relplot(x='time_elapsed_ms_cum', y='throughput_combined', hue='type',
    #             units='server_ip', estimator=None, kind='line', data=tidy_throughput)

    # LATENCY PLOTS
    tidy_latency = pd.melt(df.reset_index(),
                              id_vars=['time_elapsed_ms_cum', 'server_ip', 'key_range', 'replication_degree'],
                              value_vars=['latency', 'latency_cum'],
                              var_name='type',
                              value_name='latency_combined')
    # Point plot of latency (I like this one the best)
    sns.relplot(x='time_elapsed_ms_cum', y='latency_combined', hue='type',
                col='key_range', row='replication_degree', data=tidy_latency)
    # # Line plot of latency
    # sns.relplot(x='time_elapsed_ms_cum', y='latency_combined', hue='type',
    #             kind='line', data=tidy_latency)
    # # Multiline plot of latency
    # sns.relplot(x='time_elapsed_ms_cum', y='latency_combined', hue='type',
    #             units='server_ip', estimator=None, kind='line', data=tidy_latency)

    # OPERATION COUNT PLOTS
    tidy_count = pd.melt(df.reset_index(),
                         id_vars=['time_elapsed_ms_cum', 'server_ip', 'key_range', 'replication_degree'],
                         value_vars=['put_commit', 'get', 'put_abort', 'get_negack'],
                         var_name='cmd',
                         value_name='count')
    # Multiline plot, no estimator
    sns.relplot(x='time_elapsed_ms_cum', y='count', hue='cmd',
                units='server_ip', estimator=None, kind='line',
                col='key_range', row='replication_degree', data=tidy_count)
    # lines with 95% CI estimator around mean.
    sns.relplot(x='time_elapsed_ms_cum', y='count', hue='cmd',
                kind='line',
                col='key_range', row='replication_degree', data=tidy_count)

    plt.show()


if __name__ == '__main__':
    # Update our running thing.
    with open('results_all.json', 'r') as json_file:
        data_all = json.load(json_file)

    plot_time_series_cum(data_all)