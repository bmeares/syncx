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
        'daily_volume': int,
        'error_rate': float,
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
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    pd = import_pandas()
    runs = {
        'Baseline': ['simple', 'naive'],
        'Simples': ['simple', 'simple-backtrack', 'simple-slow-id', 'append', 'join'],
        'Iteratives': ['simple', 'daily-rowcount', 'unbounded-cpi', 'unbounded-binary', 'bounded-cpi', 'bounded-binary'],
        'Correctives': ['simple', 'simple-monthly-naive', 'simple-monthly-cpi', 'simple-monthly-binary', 'simple-monthly-bounded-simple', 'simple-monthly-bounded-cpi', 'simple-monthly-bounded-binary'],
    }
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
            plt.subplots_adjust(left=0.1, bottom=0.1, right=0.85, top=0.9, wspace=0.2, hspace=0.5)
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
            drt_ax.set_ylim([0.0, max_drt + 0.1])
            plt.ylabel("Seconds")
            drt_ax.set_title(f"Daily Runtimes of Scenario\n'{scenario}'")
            #  drt_df.to_csv(csv_path / (scenario_name + '_daily_runtime.csv'))
            #  plt.savefig(figures_path / (scenario_name + '_daily_runtime.png'), bbox_inches="tight")

            #  rt_ax = rt_figure_df.plot(x='Month')
            #  rt_ax.set_ylim([0.0, max_rt + 0.1])
            #  plt.ylabel("Seconds")
            #  rt_ax.set_title(f"Monthly Average Runtimes of Scenario\n'{scenario_name}'")
            #  rt_df.to_csv(csv_path / (scenario_name + '_monthly_runtime.csv'))
            #  plt.savefig(figures_path / (scenario_name + '_monthly_runtime.png'), bbox_inches="tight")

            #  er_ax = er_df.plot(
                #  x = 'Datetime',
                #  kind = 'line',
                #  color = [methods_colors.get(method, '#333333') for method in er_df],
            #  )
            #  er_ax.set_ylim([0.0, max_er + 10])
            #  er_ax.xaxis.set_major_locator(mdates.MonthLocator())
            #  er_ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
            #  plt.ylabel("Missing Rows")
            #  er_ax.set_title(f"Cumulative Errors of Scenario\n '{scenario_name}'")
            #  er_df.to_csv(csv_path / (scenario_name + '_errors.csv'))
            #  plt.savefig(figures_path / (scenario_name + '_errors.png'), bbox_inches="tight")

            rer_df.plot(
                x = 'Datetime',
                kind = 'line',
                color = [methods_colors.get(method, '#333333') for method in rer_df][1:],
                ax = rer_ax,
                legend = False,
            )
            rer_ax.set_ylim([min_rer, max_rer + 1])
            rer_ax.xaxis.set_major_locator(mdates.MonthLocator())
            rer_ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
            plt.ylabel("Accuracy Percentage")
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
            vl_ax.set_ylim([0, max_vl + 10])
            vl_ax.xaxis.set_major_locator(mdates.MonthLocator())
            vl_ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
            plt.ylabel("Rows Transferred")
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
            dvl_ax.set_ylim([0, max_dvl + 10])
            dvl_ax.xaxis.set_major_locator(mdates.MonthLocator())
            dvl_ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
            plt.ylabel("Rows Transferred")
            dvl_ax.set_title(f"Daily Fetched Row Volume of Scenario\n'{scenario}'")
            dvl_ax.legend(loc='upper right', bbox_to_anchor=(1.35, 1))
            #  dvl_figure_df.to_csv(csv_path / (scenario_name + '_daily_volume.csv'))
            #  plt.savefig(figures_path / (scenario_name + '_daily_volume.png'), bbox_inches="tight")

            plt.show()


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
                if metric not in metric_bounds:
                    continue
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
