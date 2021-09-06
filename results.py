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
    methods = list(fetch_methods.keys()) + list(sync_methods.keys())
    generic_dtypes = {'Datetime': 'datetime64[ns]', 'Month': 'datetime64[ns]'}
    datasets_methods_dtypes = {
        'cumulative_volume': int,
        'daily_runtime': float,
        #  'daily_volume': int,
        'error_rate': int,
        #  'monthly_runtime': float,
    }
    readable_dataset_names = {
        'cumulative_volume': 'Number of Rows',
        'daily_runtime': 'Runtime in Seconds',
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
        if run.startswith('.') or not os.path.isdir(run_dir_path):
            continue
        if run not in runs_scenarios_radar_data:
            runs_scenarios_radar_data[run] = {}
        scenarios_radar_data = runs_scenarios_radar_data[run]
        for dataset, dtypes in datasets_dtypes.items():
            totals_dfs = []

            scenarios_dir_path = run_dir_path / 'scenarios'
            for scenario in os.listdir(scenarios_dir_path):
                scenario_path = scenarios_dir_path / scenario
                if scenario.startswith('.') or not os.path.isdir(scenario_path):
                    continue
                if scenario not in scenarios_radar_data:
                    scenarios_radar_data[scenario] = {'method': [], 'metric': [], 'number': []}
                csv_file_path = scenario_path / 'csv' / (scenario + '_' + dataset + '.csv')
                _df = pd.read_csv(csv_file_path, index_col=0)
                df = _df.astype({col: val for col, val in dtypes.items() if col in _df})
                for col in df:
                    if col not in master_runs_data:
                        master_runs_data[col] = df[col]
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
                        color = (
                            "C" + str(len([m for m in methods_colors if m != 'naive']))
                        ) if method != 'naive' else '#555555'
                        methods_colors[method] = color
                        num_methods = len(methods_colors)
                        if num_methods <= 10:
                            linestyle = 'solid'
                        elif num_methods <= 20:
                            linestyle = 'dashed'
                        elif num_methods <= 30:
                            linestyle = 'dotted'
                        else:
                            linestyle = 'dashdot'
                        methods_linestyles[method] = linestyle

            all_totals_df = pd.concat(totals_dfs)
            all_totals_df.index.name = 'Scenario'
            existing_cols = all_totals_df.columns
            all_totals_df['Scenario'] = all_totals_df.index
            all_totals_df = all_totals_df[['Scenario'] + list(existing_cols)]
            
            #  print(all_totals_df)
            #  print(all_totals_df.to_latex(
                #  multicolumn=True, multirow=True, longtable=False, index_names=False, index=False,
                #  float_format="%.2f",
                #  caption=(
                    #  'Total ' + readable_dataset_names[dataset] + ' for Methods '
                    #  + items_str(list(existing_cols)))
                #  )
            #  )
            #  input()

    #  determine_bounds(runs_scenarios_radar_data)
    make_line_chart(master_runs_data)
    make_radar_chart(runs_scenarios_radar_data)
    return 0

scenarios_preset_metric_bounds = {}

def make_line_chart(master_runs_data):
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas()
    master_df = pd.DataFrame(master_runs_data)
    runs = {
        'Baseline': ('simple', 'naive'),
        'Simples': ('simple', 'simple-backtrack', 'simple-slow-id', 'append', 'join'),
        'Iteratives': ('simple', 'daily-rowcount', 'unbounded-simple', 'unbounded-cpi', 'unbounded-binary', 'bounded-simple', 'bounded-cpi', 'bounded-binary'),
        'Correctives': ('simple', 'simple-monthly-naive', 'simple-monthly-cpi', 'simple-monthly-binary', 'simple-monthly-bounded-simple', 'simple-monthly-bounded-cpi', 'simple-monthly-bounded-binary'),
    }
    print(master_df.columns)
    print(master_df)
    input()


def normalize_value(value, min_value, max_value, better='higher'):
    if min_value == max_value and min_value == 100:
        min_value = 0
    if value < min_value:
        value = min_value
    elif value > max_value:
        value = max_value
    norm_val = (value - min_value) / (max_value - min_value)
    final_val = (norm_val if better == 'higher' else (1.0 - norm_val))
    ### Bump 0 to 0.05 for visibility
    if final_val == 0:
        return 0.05
    return final_val


def determine_bounds(runs_scenarios_radar_data):
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas()
    for run, scenarios_radar_data in runs_scenarios_radar_data.items():
        for scenario, radar_data in scenarios_radar_data.items():
            if scenario in scenarios_preset_metric_bounds:
                continue
            radar_df = pd.DataFrame(radar_data)
            simples = radar_df.where(radar_df['method'] == 'simple')
            naives = radar_df.where(radar_df['method'] == 'naive')
            simple_cumulative_volume = simples.where(simples['metric'] == 'cumulative_volume')['number'].dropna().reset_index(drop=True)[0]
            naive_cumulative_volume = naives.where(naives['metric'] == 'cumulative_volume')['number'].dropna().reset_index(drop=True)[0]

            simple_daily_runtime = simples.where(simples['metric'] == 'daily_runtime')['number'].dropna().reset_index(drop=True)[0]
            naive_daily_runtime = naives.where(naives['metric'] == 'daily_runtime')['number'].dropna().reset_index(drop=True)[0]
            scenarios_preset_metric_bounds[scenario] = {
                'error_rate': (0, 100),
                'cumulative_volume': (simple_cumulative_volume, simple_cumulative_volume * 1.25),
                'daily_runtime': (simple_daily_runtime, simple_daily_runtime * 1.25),
            }

            

def make_radar_chart(runs_scenarios_radar_data):
    import matplotlib.pyplot as plt
    from meerschaum.utils.packages import import_pandas
    import numpy as np
    pd = import_pandas()
    higher_metrics = ('error_rate',)
    for run, scenarios_radar_data in runs_scenarios_radar_data.items():
        for scenario, radar_data in scenarios_radar_data.items():
            radar_df = pd.DataFrame(radar_data)
            daily_runtime_df = radar_df.where(radar_df['metric'] == 'daily_runtime').dropna().sort_values(by='number').reset_index(drop=True)
            daily_runtime_pt = pd.pivot_table(daily_runtime_df, values='number', index=['metric'], columns=['method'])[daily_runtime_df['method']]

            cumulative_volume_df = radar_df.where(radar_df['metric'] == 'cumulative_volume').dropna().sort_values(by='number').reset_index(drop=True)
            cumulative_volume_pt = pd.pivot_table(cumulative_volume_df, values='number', index=['metric'], columns=['method'])[cumulative_volume_df['method']]

            error_rate_df = radar_df.where(radar_df['metric'] == 'error_rate').dropna().sort_values(by='number').reset_index(drop=True)
            error_rate_pt = pd.pivot_table(error_rate_df, values='number', index=['metric'], columns=['method'])[error_rate_df['method']]

            fig, (rt_ax, cv_ax, er_ax) = plt.subplots(1, 3, figsize=(16, 8))

            daily_runtime_pt.plot(
                kind='bar', title=f"Total Runtime for Scenario\n'{scenario}'", legend=False,
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
                ylabel='% of Rows Sychronized', xlabel='',
                color=[methods_colors.get(method, '#333333') for method in error_rate_pt],
                edgecolor=['#333333' for method in daily_runtime_pt],
                #  linestyle=[methods_linestyles.get(method, 'dashdot') for method in error_rate_pt],
                ax=er_ax,
            )
            er_ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1))
            er_ax.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
            er_ax.set_ylim(0, 100)

            plt.show()

            simples = radar_df.where(radar_df['method'] == 'simple')
            simple_cumulative_volume = simples.where(simples['metric'] == 'cumulative_volume')['number'].dropna().reset_index(drop=True)[0]
            simple_daily_runtime = simples.where(simples['metric'] == 'daily_runtime')['number'].dropna().reset_index(drop=True)[0]
            metric_bounds = {
                'error_rate': (0, 100),
                'cumulative_volume': (simple_cumulative_volume, simple_cumulative_volume * 1.25),
                'daily_runtime': (simple_daily_runtime, simple_daily_runtime * 1.25),
            }

            metrics = radar_df['metric'].unique()
            for metric in metrics:
                metric_vals = radar_df.where(radar_df['metric'] == metric)['number']
                #  _mmin, _mmax = metric_vals.min(), metric_vals.max()
                #  mmin, mmax = scenarios_preset_metric_bounds[scenario][metric]
                mmin, mmax = metric_bounds[metric]
            
                normalized_vals = metric_vals.apply(lambda x: normalize_value(x, mmin, mmax, better=('higher' if metric in higher_metrics else 'lower')))
                radar_df['number'].update(normalized_vals[normalized_vals.notnull()])

            pt = pd.pivot_table(radar_df, values='number', index=['metric'], columns=['method'])
            fig = plt.figure()
            ax = fig.add_subplot(111, projection="polar")
            theta = np.arange(len(pt))/float(len(pt))*2.*np.pi
            lines = []
            for i, method in enumerate(pt):
                l, = ax.plot(theta, pt[method], color=methods_colors[method], label=method, linewidth=1.5)
                angles = np.linspace(0, 2 * np.pi, len(metrics), endpoint=False).tolist()
                values = radar_df.where(radar_df['method'] == method)['number'].dropna().tolist()
                ax.fill(angles, values, color=methods_colors[method], alpha=0.1)
                lines.append(l)

            def _close_line(line):
                x, y = line.get_data()
                x = np.concatenate((x, [x[0]]))
                y = np.concatenate((y, [y[0]]))
                line.set_data(x, y)

            for l in lines:
                _close_line(l)
            ax.set_xticks(theta)
            ax.set_xticklabels(pt.index)
            ax.tick_params(axis='y', labelsize=8)
            ax.tick_params(axis='x', labelsize=8)
            ax.set_xticklabels(['        Bandwidth', 'Runtime', 'Accuracy'])
            plt.legend(loc='lower right', bbox_to_anchor=(1.25, 0))
            plt.title(f"Relative Performance for Scenario\n'{scenario}'")
            print(run, scenario)
            plt.show()



if __name__ == "__main__":
    sys.exit(main(sys.argv))
