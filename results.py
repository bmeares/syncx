#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Read the CSVs and produce the results.
"""

import os
import sys
import pathlib

methods_colors = {}
methods_linestyles = {}
methods_markers = {}
runs = {
    'baseline': ['simple', 'naive'],
    'simples': ['simple', 'simple-backtrack', 'simple-slow-id', 'append', 'join'],
    #  'Iteratives': [
        #  'simple', 'unbounded-daily-rowcount', 'unbounded-simple', 'unbounded-cpi',
        #  'unbounded-binary', 'bounded-daily-rowcount', 'bounded-simple', 'bounded-cpi',
        #  'bounded-binary'
    #  ],
    'unbounded': [
        'simple', 'unbounded-simple', 'unbounded-daily-rowcount', 'unbounded-binary', 'unbounded-cpi',
    ],
    'bounded': [
        'simple', 'bounded-simple', 'bounded-daily-rowcount', 'bounded-binary', 'bounded-cpi',
    ],
    'unbounded-correctives': [
        'simple', 'simple-monthly-naive', 'simple-monthly-iterative-simple', 'simple-monthly-daily-rowcount', 'simple-monthly-binary', 'simple-monthly-cpi',
    ],
    'bounded-correctives': ['simple', 'simple-monthly-bounded-simple', 'simple-monthly-bounded-daily-rowcount', 'simple-monthly-bounded-binary', 'simple-monthly-bounded-cpi'],
    'bounded-unbounded': ['simple', 'unbounded-simple', 'bounded-simple'],
    'binary-daily-rowcount': ['simple', 'bounded-binary', 'bounded-daily-rowcount'],
    #  'winners': ['simple', 'simple-monthly-bounded-simple', 'simple-monthly-bounded-cpi', 'bounded-daily-rowcount'],
    'winners': ['simple', 'simple-backtrack', 'join', 'bounded-daily-rowcount', 'unbounded-cpi', 'simple-monthly-bounded-simple'],
    'zummary': ['simple', 'unbounded-daily-rowcount', 'simple-monthly-bounded-simple'],
    #  'Correctives': [
        #  'simple', 'simple-monthly-naive', 'simple-monthly-daily-rowcount', 'simple-monthly-cpi',
        #  'simple-monthly-binary', 'simple-monthly-bounded-simple',
        #  'simple-monthly-bounded-daily-rowcount', 'simple-monthly-bounded-cpi',
        #  'simple-monthly-bounded-binary',
    #  ],
}
runs['all'] = []
for run, strats in runs.items():
    runs['all'] += [strat for strat in strats if strat not in runs['all']]


def main(argv):
    from meerschaum.config._patch import apply_patch_to_config
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.misc import items_str
    from methods import fetch_methods, sync_methods
    from meerschaum.utils.formatting import pprint
    pd = import_pandas()
    results_dir_path = (
        (pathlib.Path.home() / 'syncx_results') if len(argv) == 1
        else pathlib.Path(argv[1])
    )
    if not results_dir_path.exists():
        print('Usage: python results.py {results directory}', file=sys.stderr)
        return 1
    figures_dir_path = results_dir_path / 'figures'
    figures_dir_path.mkdir(parents=True, exist_ok=True)
    methods = list(fetch_methods.keys()) + list(sync_methods.keys())
    generic_dtypes = {'Datetime': 'datetime64[ns]', 'Month': 'datetime64[ns]'}
    datasets_methods_dtypes = {
        'cumulative_volume': int,
        'daily_runtime': float,
        'daily_volume': int,
        'error_rate': float,
        #  'monthly_runtime': float,
    }
    readable_dataset_names = {
        'cumulative_volume': 'Number of Rows',
        'daily_runtime': 'Run-time in Seconds',
        'errors': 'Number of Errors',
    }
    datasets_dtypes = {
        dataset: apply_patch_to_config(
            {
                method: datasets_methods_dtypes[dataset]
                for method in methods
            }, generic_dtypes
        )
        for dataset in datasets_methods_dtypes
    }
    #  summary_data = {'method': [], 'avg total x time': [], 'avg'}
    #  methods_total_x_times = {method: [] for method in methods}


    master_runs_data = {}
    runs_scenarios_radar_data = {}
    for run in sorted(os.listdir(results_dir_path)):
        run_dir_path = results_dir_path / run
        if run.startswith('.') or run == 'figures' or not os.path.isdir(run_dir_path):
            continue
        if run not in runs_scenarios_radar_data:
            runs_scenarios_radar_data[run] = {}
        scenarios_radar_data = runs_scenarios_radar_data[run]
        for dataset, dtypes in datasets_dtypes.items():
            if dataset not in master_runs_data:
                master_runs_data[dataset] = {}
            totals_dfs = []

            scenarios_dir_path = run_dir_path / 'scenarios'
            for scenario in os.listdir(scenarios_dir_path):
                if scenario not in master_runs_data[dataset]:
                    master_runs_data[dataset][scenario] = {}
                scenario_path = scenarios_dir_path / scenario
                if scenario.startswith('.') or not os.path.isdir(scenario_path):
                    continue
                if scenario not in scenarios_radar_data:
                    scenarios_radar_data[scenario] = {'method': [], 'metric': [], 'number': []}
                csv_file_path = scenario_path / 'csv' / (scenario + '_' + dataset + '.csv')
                _df = pd.read_csv(csv_file_path, index_col=0)
                df = _df.astype({col: val for col, val in dtypes.items() if col in _df})
                for col in df:
                    if col not in master_runs_data[dataset][scenario]:
                        master_runs_data[dataset][scenario][col] = df[col]
                _total_df = pd.DataFrame({
                    col: {scenario: (
                        df[col].mean() if dataset == 'error_rate'
                        else (df[col].sum() if dataset == 'daily_runtime' else df[col].max())
                    )}
                    for col in df if col not in generic_dtypes
                })
                totals_dfs.append(_total_df)
                methods = list(_total_df.columns)
                for method in methods:
                    scenarios_radar_data[scenario]['method'].append(method)
                    scenarios_radar_data[scenario]['metric'].append(dataset)
                    scenarios_radar_data[scenario]['number'].append(_total_df[method][0])
                    if method not in methods_colors:
                        #  color = (
                            #  "C" + str(len([m for m in methods_colors if m != 'naive']))
                        #  ) if method != 'naive' else '#555555'
                        #  colors = ['#e6194B', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4', '#42d4f4', '#f032e6', '#bfef45', '#fabed4', '#469990', '#dcbeff', '#9A6324', '#fffac8', '#800000', '#aaffc3', '#808000', '#ffd8b1', '#000075', '#a9a9a9', '#ffffff', '#000000']
                        colors = ['blue', 'dimgrey', 'orange', 'red', 'maroon', 'darkcyan', 'gold', 'green', 'purple', 'teal', 'deeppink', 'steelblue', 'darkgreen', 'tan', 'springgreen', 'cadetblue', 'mediumorchid', 'midnightblue', 'mediumvioletred', 'coral', 'darkslategrey', 'yellowgreen', 'lightsteelblue', 'tomato', 'plum', 'chocolate']
                        color = colors[len(methods_colors) % len(colors)]
                        methods_colors[method] = color
                        markers = ['s', 'o', 'v', 'x', '^', 'D', '*', 'd']
                        marker = markers[len(methods_colors) % len(markers)]
                        methods_markers[method] = marker
                        num_methods = len(methods_colors)
                        if num_methods <= len(colors):
                            linestyle = 'solid'
                        elif num_methods <= 2 * len(colors):
                            linestyle = 'dashed'
                        elif num_methods <= 3 * len(colors):
                            linestyle = 'dotted'
                        else:
                            linestyle = 'dashdot'
                        methods_linestyles[method] = linestyle

            all_totals_df = pd.concat(totals_dfs)
            all_totals_df.index.name = 'Scenario'
            existing_cols = all_totals_df.columns
            all_totals_df['Scenario'] = all_totals_df.index
            all_totals_df = all_totals_df[['Scenario'] + list(existing_cols)]
            
    make_line_chart(master_runs_data, figures_dir_path)
    make_radar_chart(make_radar_data(runs_scenarios_radar_data), figures_dir_path)
    return 0

def _set_line_markers(_ax, _df):
    import random
    lines = _ax.get_lines()
    methods = list(_df.columns)[1:]
    for i, (line, method) in enumerate(zip(lines, methods)):
        line.set_marker(methods_markers.get(method, '+'))
        line.set_markevery(((i / len(lines)) / 10, 0.1))
        line.set_linestyle(methods_linestyles.get(method, 'dotted'))



def make_radar_data(runs_scenarios_radar_data):
    from meerschaum.utils.formatting import pprint
    _skip_metrics = {'daily_volume'}
    new_radar_data = {run: {} for run in runs}
    _scenarios_radar_data = {}
    for _run in runs_scenarios_radar_data:
        for scenario in runs_scenarios_radar_data[_run]:
            if scenario not in _scenarios_radar_data:
                _scenarios_radar_data[scenario] = {}
            for col in runs_scenarios_radar_data[_run][scenario]:
                if col not in _scenarios_radar_data[scenario]:
                    _scenarios_radar_data[scenario][col] = []
                _scenarios_radar_data[scenario][col] += runs_scenarios_radar_data[_run][scenario][col].copy()

    for scenario in _scenarios_radar_data:
        for i, strategy in enumerate(_scenarios_radar_data[scenario]['method']):
            for run in runs:
                if (
                    strategy in runs[run]
                    and new_radar_data[run].get(scenario, {}).get('method', []).count(strategy) < 5
                    and _scenarios_radar_data[scenario]['metric'][i] not in _skip_metrics
                ):
                    if scenario not in new_radar_data[run]:
                        new_radar_data[run][scenario] = {'method': [], 'metric': [], 'number': []}
                    for col in new_radar_data[run][scenario]:
                        new_radar_data[run][scenario][col].append(_scenarios_radar_data[scenario][col][i])

    incomplete_runs = []
    for run in new_radar_data:
        for scenario in new_radar_data[run]:
            if len(set(new_radar_data[run][scenario]['method'])) != len(runs[run]):
                incomplete_runs.append((run, scenario))

    for run, scenario in incomplete_runs:
        del new_radar_data[run][scenario]
    return new_radar_data



scenarios_preset_metric_bounds = {}

def make_line_chart(master_runs_data, figures_dir_path):
    from meerschaum.utils.packages import import_pandas
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    pd = import_pandas()
    #  runs['All'] = runs['Baseline'] + runs['Simples'] + runs['Iteratives'] + runs['Correctives']

    for run, strategies in runs.items():

        for scenario in master_runs_data['error_rate']:

            missing = False
            for strat in strategies:
                if strat not in master_runs_data['daily_runtime'][scenario]:
                    missing = True
            if missing:
                continue
            drt_df = pd.DataFrame(master_runs_data['daily_runtime'][scenario])[['Datetime'] + strategies]
            #  rt_df = pd.DataFrame(master_runs_data['monthly_runtime'])
            #  er_df = pd.DataFrame(master_runs_data['errors'])
            rer_df = pd.DataFrame(master_runs_data['error_rate'][scenario])[['Datetime'] + strategies]
            vl_df = pd.DataFrame(master_runs_data['cumulative_volume'][scenario])[['Datetime'] + strategies]
            dvl_df = pd.DataFrame(master_runs_data['daily_volume'][scenario])[['Datetime'] + strategies]
            max_drt = max(drt_df[strategies].max())
            #  max_rt = max(rt_df[strategies].max())
            #  max_er = max(er_df[strategies].max())
            min_rer = max(min(rer_df[strategies].min()) - 10, 0)
            max_rer = min(max(rer_df[strategies].max()), 100)
            max_vl = max(vl_df[strategies].max())
            max_dvl = max(dvl_df[strategies].max())

            ### Build a 4x4 graph
            fig, axs = plt.subplots(2, 2, figsize=(16, 9))
            #  fig.subplots_adjust(right=0.2)
            plt.subplots_adjust(left=0.1, bottom=0.1, right=0.78, top=0.9, wspace=0.2, hspace=0.5)
            #  fig.tight_layout(h_pad=4)
            drt_ax = axs[0, 0]
            dvl_ax = axs[0, 1]
            vl_ax = axs[1, 0]
            rer_ax = axs[1, 1]

            ### Create one figure per scenario with all of the methods.
            drt_df.plot(
                x = 'Datetime',
                color = [methods_colors.get(method, '#333333') for method in drt_df][1:],
                ax = drt_ax,
                legend = False,
            )

            _set_line_markers(drt_ax, drt_df)
            drt_ax.set_ylim([0.0, max_drt + 0.1])
            drt_ax.set_ylabel("Seconds")
            drt_ax.set_title(f"Daily Run-times of Scenario\n'{scenario}'")

            rer_df.plot(
                x = 'Datetime',
                kind = 'line',
                color = [methods_colors.get(method, '#333333') for method in rer_df][1:],
                ax = rer_ax,
                legend = False,
            )
            _set_line_markers(rer_ax, rer_df)
            rer_ax.set_ylim([min_rer, max_rer + 1])
            rer_ax.xaxis.set_major_locator(mdates.MonthLocator())
            rer_ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
            rer_ax.set_ylabel("Accuracy Percentage")
            rer_ax.set_title(f"Running Accuracy Rate of Scenario\n'{scenario}'")
            #  rer_df.to_csv(csv_path / (scenario_name + '_error_rate.csv'))
            #  plt.savefig(figures_path / (scenario_name + '_error_rate.png'), bbox_inches="tight")


            vl_df.plot(
                x ='Datetime',
                kind = 'line',
                color = [methods_colors.get(method, '#333333') for method in vl_df][1:],
                ax = vl_ax,
                legend = False,
            )
            _set_line_markers(vl_ax, vl_df)
            vl_ax.set_ylim([0, int(max_vl * 1.02)])
            vl_ax.xaxis.set_major_locator(mdates.MonthLocator())
            vl_ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
            vl_ax.set_ylabel("Rows Transferred")
            vl_ax.set_title(f"Cumulative Fetched Row Volume of Scenario\n'{scenario}'")
            #  vl_figure_df.to_csv(csv_path / (scenario_name + '_cumulative_volume.csv'))
            #  plt.savefig(figures_path / (scenario_name + '_cumulative_volume.png'), bbox_inches="tight")

            dvl_df.plot(
                x = 'Datetime',
                kind = 'line',
                color = [methods_colors.get(method, '#333333') for method in dvl_df][1:],
                ax = dvl_ax,
                legend = False,
            )
            _set_line_markers(dvl_ax, dvl_df)
            dvl_ax.set_ylim([0, int(max_dvl * 1.05)])
            dvl_ax.xaxis.set_major_locator(mdates.MonthLocator())
            dvl_ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
            dvl_ax.set_ylabel("Rows Transferred")
            dvl_ax.set_title(f"Daily Fetched Row Volume of Scenario\n'{scenario}'")
            dvl_ax.legend(loc='upper left', bbox_to_anchor=(1.0, 1))
            #  dvl_figure_df.to_csv(csv_path / (scenario_name + '_daily_volume.csv'))
            #  plt.savefig(figures_path / (scenario_name + '_daily_volume.png'), bbox_inches="tight")

            plt.savefig(figures_dir_path / (run + '_' + scenario + '_lines.png'), bbox_inches="tight")
            #  plt.show()


def normalize_value(value, min_value, max_value, better='higher', adjust=False):
    if min_value == max_value and min_value == 100:
        min_value = 0
    if value < min_value and adjust:
        value = min_value
    elif value > max_value and adjust:
        value = max_value
    norm_val = (value - min_value) / (max_value - min_value)
    final_val = (norm_val if better == 'higher' else (1.0 - norm_val))
    ### Bump 0 to 0.05 for visibility
    if final_val == 0 and adjust:
        return 0.05
    return final_val


def make_radar_chart(runs_scenarios_radar_data, figures_dir_path):
    import matplotlib.pyplot as plt
    from meerschaum.utils.packages import import_pandas
    import numpy as np
    pd = import_pandas()
    higher_metrics = ('error_rate',)

    runs_scenarios_radar_data_tmp = {}
    runs_scenarios_radar_data_tmp['all'] = runs_scenarios_radar_data['all']

    for run, scenarios_radar_data in runs_scenarios_radar_data.items():
        print(run)
        for scenario, radar_data in scenarios_radar_data.items():
            radar_df = pd.DataFrame(radar_data)
            daily_runtime_df = radar_df.where(radar_df['metric'] == 'daily_runtime').dropna().sort_values(by='number').reset_index(drop=True)
            daily_runtime_pt = pd.pivot_table(daily_runtime_df, values='number', index=['metric'], columns=['method'])[daily_runtime_df['method']]

            cumulative_volume_df = radar_df.where(radar_df['metric'] == 'cumulative_volume').dropna().sort_values(by='number').reset_index(drop=True)
            cumulative_volume_pt = pd.pivot_table(cumulative_volume_df, values='number', index=['metric'], columns=['method'])[cumulative_volume_df['method']]

            error_rate_df = radar_df.where(radar_df['metric'] == 'error_rate').dropna().sort_values(by='number').reset_index(drop=True)
            error_rate_pt = pd.pivot_table(error_rate_df, values='number', index=['metric'], columns=['method'])[error_rate_df['method']]

            fig, (rt_ax, cv_ax, er_ax) = plt.subplots(1, 3, figsize=(16, 8))
            fig.subplots_adjust(bottom=0.2)

            daily_runtime_pt.plot(
                kind='bar', title=f"Total Run-time for Scenario\n'{scenario}'", legend=False,
                ylabel='Seconds', xlabel='',
                color=[methods_colors.get(method, '#333333') for method in daily_runtime_pt],
                edgecolor=['#333333' for method in daily_runtime_pt],
                linestyle='solid',
                #  linestyle=[methods_linestyles.get(method, 'dashdot') for method in daily_runtime_pt],
                ax=rt_ax,
            )
            rt_ax.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)

            cumulative_volume_pt.plot(
                kind='bar', title=f"Total Rows Fetched for Scenario\n'{scenario}'", legend=False,
                ylabel='Rows', xlabel='',
                color=[methods_colors.get(method, '#333333') for method in cumulative_volume_pt],
                edgecolor=['#333333' for method in cumulative_volume_pt],
                #  linestyle=[methods_linestyles.get(method, 'dashdot') for method in cumulative_volume_pt],
                ax=cv_ax,
            )
            cv_ax.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)

            error_rate_pt.plot(
                kind='bar', title=f"Average Accuracy Rate for Scenario\n'{scenario}'", legend=False,
                ylabel='Accuracy Percentage', xlabel='',
                color=[methods_colors.get(method, '#333333') for method in error_rate_pt],
                edgecolor=['#333333' for method in daily_runtime_pt],
                #  linestyle=[methods_linestyles.get(method, 'dashdot') for method in error_rate_pt],
                ax=er_ax,
            )
            er_ax.legend(loc='upper left', ncol=1, bbox_to_anchor=(1.0, 1.0), fancybox=True)
            er_ax.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
            er_ax.set_ylim(0, 100)

            plt.subplots_adjust(left=0.1, bottom=0.1, right=0.78, top=0.9, wspace=0.3, hspace=0.5)
            #  plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
            plt.savefig(figures_dir_path / (run + '_' + scenario + '_bars.png'), bbox_inches="tight")
            #  plt.show()

            simples = radar_df.where(radar_df['method'] == 'simple')
            simple_cumulative_volume = simples.where(simples['metric'] == 'cumulative_volume')['number'].dropna().reset_index(drop=True)[0]
            simple_daily_runtime = simples.where(simples['metric'] == 'daily_runtime')['number'].dropna().reset_index(drop=True)[0]
            metric_bounds = {
                'error_rate': (0, 100),
                'cumulative_volume': (simple_cumulative_volume, simple_cumulative_volume * 15.0),
                'daily_runtime': (simple_daily_runtime, simple_daily_runtime * 15.0),
            }

            metrics = radar_df['metric'].unique()
            choice_df = radar_df.copy()
            for metric in metrics:
                if metric not in metric_bounds:
                    continue
                metric_vals = radar_df.where(radar_df['metric'] == metric)['number']
                #  _mmin, _mmax = metric_vals.min(), metric_vals.max()
                #  mmin, mmax = scenarios_preset_metric_bounds[scenario][metric]
                mmin, mmax = metric_bounds[metric]
            
                normalized_vals = metric_vals.apply(lambda x: normalize_value(x, mmin, mmax, better=('higher' if metric in higher_metrics else 'lower'), adjust=True))
                choice_vals = metric_vals.apply(lambda x: normalize_value(x, mmin, mmax, better=('higher' if metric in higher_metrics else 'lower'), adjust=False))
                radar_df['number'].update(normalized_vals[normalized_vals.notnull()])
                choice_df['number'].update(choice_vals[choice_vals.notnull()])
            for priorities in [1, 2, 3]:
                generate_weighted_scores(choice_df, run, scenario, figures_dir_path, priorities=priorities)

            pt = pd.pivot_table(radar_df, values='number', index=['metric'], columns=['method'])
            fig = plt.figure(figsize=(12, 8))
            ax = fig.add_subplot(111, projection="polar")
            theta = np.arange(len(pt))/float(len(pt))*2.*np.pi
            lines = []
            for i, method in enumerate(pt):
                l, = ax.plot(theta, pt[method], color=methods_colors[method], label=method, linewidth=1.5)
                angles = np.linspace(0, 2 * np.pi, len(metrics), endpoint=False).tolist()
                values = radar_df.where(radar_df['method'] == method)['number'].dropna().tolist()
                ax.fill(angles, values, color=methods_colors[method], alpha=0.1)
                lines.append((l, method))

            def _close_line(line):
                x, y = line.get_data()
                x = np.concatenate((x, [x[0]]))
                y = np.concatenate((y, [y[0]]))
                line.set_data(x, y)

            for i, (l, method) in enumerate(lines):
                _close_line(l)
                l.set_marker(methods_markers.get(method, '+'))
                l.set_markevery(((i / len(lines)) / 10, 0.1))
                l.set_linestyle(methods_linestyles.get(method, 'dotted'))

            ax.set_xticks(theta)
            ax.set_xticklabels(pt.index)
            ax.tick_params(axis='y', labelsize=8)
            ax.tick_params(axis='x', labelsize=8)
            ax.set_xticklabels(['        Bandwidth', 'Run-time', 'Accuracy'])
            plt.subplots_adjust(top=0.9, bottom=0.05, right=0.65, left=0.05, hspace=0, wspace=0)
            #  plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=0.5)
            plt.legend(loc='upper left', bbox_to_anchor=(1.1, 1.0))
            plt.title(f"Relative Performance for Scenario\n'{scenario}'")
            print(run, scenario)
            #  plt.show()
            plt.savefig(figures_dir_path / (run + '_' + scenario + '_radar.png'), bbox_inches="tight")


def generate_weighted_scores(radar_df, run, scenario, figures_dir_path, priorities=3):
    import matplotlib.pyplot as plt
    import duckdb
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas()
    balanced_query = """
    SELECT method, AVG(number) AS "score"
    FROM radar_df
    GROUP BY method
    ORDER BY "score" DESC
    """
    metrics_prefix = """
    WITH bws AS (
        SELECT method, AVG(number) AS 'avg'
        FROM radar_df
        WHERE radar_df.metric = 'cumulative_volume'
        GROUP BY method
    ), rts AS (
        SELECT method, AVG(number) AS 'avg'
        FROM radar_df
        WHERE radar_df.metric == 'daily_runtime'
        GROUP BY method
    ), acs AS (
        SELECT method, AVG(number) AS 'avg'
        FROM radar_df
        WHERE radar_df.metric == 'error_rate'
        GROUP BY method
    ), scores AS (
        SELECT bws.method, bws.avg AS 'bw_avg', rts.avg AS 'rt_avg', acs.avg AS 'ac_avg'
        FROM bws
        INNER JOIN rts ON rts.method = bws.method
        INNER JOIN acs ON acs.method = bws.method
    )
    """

    if priorities == 3:
        low = 1 / 7
        medium = low * 2
        high = low * 4
        bottom = low
        top = 1.0 - (2 * low)
    elif priorities == 2:
        low = 0.0
        medium = 1 / 3
        high = 1.0 - medium
        bottom = 0.0
        top = 1.0
    elif priorities == 1:
        low = 0.0
        medium = 0.0
        high = 1.0
        bottom = low
        top = high
    else:
        raise Exception("Incorrect number of priorities")
    metric_weights = {
        'Run-time': (bottom, top, bottom),
        'Run-time, Bandwidth, Accuracy': (medium, high, low),
        'Run-time, Accuracy, Bandwidth': (low, high, medium),
        'Bandwidth, Run-time, Accuracy': (high, medium, low),
        'Bandwidth': (top, bottom, bottom),
        'Bandwidth, Accuracy, Run-time': (high, low, medium),
        'Accuracy, Run-time, Bandwidth': (low, medium, high),
        'Accuracy, Bandwidth, Run-time': (medium, low, high),
        'Accuracy': (bottom, bottom, top),
    }

    fig, axs = plt.subplots(3, 3, figsize=(16, 9)) if priorities != 1 else plt.subplots(1, 3, figsize=(16, 5))
    fig.suptitle(f"Choice Indices Weighted for {priorities} Priorit{('ies' if priorities != 1 else 'y')}\nof Scenario '{scenario}'")
    plt.subplots_adjust(left=0.2, bottom=0.1, right=0.78, top=0.8, wspace=0.2, hspace=0.2)

    if priorities != 1:
        top_right_ax = axs[0, 2]
        top_middle_ax = axs[0, 1]
        _ax = [axs[0, 0], axs[1, 0], axs[2, 0], axs[0, 1], axs[1, 1], axs[2, 1], axs[0, 2], axs[1, 2], axs[2, 2]]
        middle_left_ax = axs[1, 0]
        top_middle_ax.text(-0.33, 1.5, f"First Priority ({round(100 * high, 2)}" + "%)", fontsize='x-large')
    else:
        top_right_ax = axs[-1]
        top_middle_ax = axs[1]
        _ax = axs
        middle_left_ax = None

    first_priority_y = 1.25 if priorities != 1 else 1.08
    top_middle_ax.text(-0.2, first_priority_y, "Bandwidth", fontsize='large', fontweight='semibold')
    top_middle_ax.text(-1.36, first_priority_y, "Run-time", fontsize='large', fontweight='semibold')
    top_middle_ax.text(1.02, first_priority_y, "Accuracy", fontsize='large', fontweight='semibold')
    if middle_left_ax is not None:
        middle_left_ax.text(-1.6, 0.5, "Second\nPriority\n" + f"({round(100 * medium, 2)}" + "%)", fontsize='x-large')
        middle_left_ax.text(-1.2, 0.5, "Bandwidth", fontsize='large', fontweight='semibold')
        middle_left_ax.text(-1.2, 1.7, "Run-time", fontsize='large', fontweight='semibold')
        middle_left_ax.text(-1.2, -0.7, "Accuracy", fontsize='large', fontweight='semibold')

    for a in _ax:
        a.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
        a.set_ylim([0.0, 1.0])

    balanced_df = duckdb.query(balanced_query).to_df()
    balanced_pt = pd.pivot_table(balanced_df, values='score', columns=['method'])[balanced_df['method']]

    #  balanced_pt.plot(
        #  kind='bar', title=f"Balanced Choice Index Rankings\nfor Scenario '{scenario}'", legend=False,
        #  ylabel='Index Score', xlabel='',
        #  color=[methods_colors.get(method, '#333333') for method in balanced_pt],
        #  edgecolor=['#333333' for method in balanced_pt],
        #  linestyle='solid',
        #  ax=bl_ax,
    #  )

    print(f"Balanced index for scenario '{scenario}'")
    print(balanced_df)
    skipped = 0
    for i, (met, (bw, rt, ac)) in enumerate(metric_weights.items()):
        if ',' in met and priorities == 1:
            skipped += 1
            continue
        query = metrics_prefix + f"""
        SELECT method, (({bw} * bw_avg) + ({rt} * rt_avg) + ({ac} * ac_avg)) AS "score"
        FROM scores
        ORDER BY "score" DESC
        """
        weighted_df = duckdb.query(query).to_df()
        weighted_pt = pd.pivot_table(weighted_df, values='score', columns=['method'])[weighted_df['method']]
        weighted_pt.plot(
            kind='bar', title="", legend=False,
            ylabel='Score', xlabel='',
            color=[methods_colors.get(method, '#333333') for method in weighted_pt],
            edgecolor=['#333333' for method in weighted_pt],
            linestyle='solid',
            #  linestyle=[methods_linestyles.get(method, 'dashdot') for method in daily_runtime_pt],
            ax=_ax[i - skipped],
        )

        print(f"Weighted index for scenario '{scenario}' by metric '{met}':")
        print(weighted_df)

    top_right_ax.legend(loc='upper left', ncol=1, bbox_to_anchor=(1.0, 1.0), fancybox=True)
    #  plt.show()
    plt.savefig(figures_dir_path / (run + '_' + scenario + f'_choice_{priorities}_priorities.png'), bbox_inches="tight")



if __name__ == "__main__":
    sys.exit(main(sys.argv))
