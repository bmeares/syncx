#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Read the CSVs and produce the results.
"""

import os
import sys
import pathlib

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


    for run in os.listdir(results_dir_path):
        run_dir_path = results_dir_path / run
        if run.startswith('.') or not os.path.isdir(run_dir_path):
            continue
        scenarios_radar_data = {}
        for dataset, dtypes in datasets_dtypes.items():
            scenarios_dfs = []
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
                scenarios_dfs.append(df)
                _total_df = pd.DataFrame({
                    col: {scenario: (
                        df[col].max() if 'daily' not in dataset
                        else df[col].sum()
                    )}
                    for col in df if col not in generic_dtypes
                })
                totals_dfs.append(_total_df)
                methods = list(_total_df.columns)
                for method in methods:
                    scenarios_radar_data[scenario]['method'].append(method)
                    scenarios_radar_data[scenario]['metric'].append(dataset)
                    scenarios_radar_data[scenario]['number'].append(_total_df[method][0])

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

        make_radar_chart(run, scenarios_radar_data)
    return 0


def make_radar_chart(run, scenarios_radar_data):
    import matplotlib.pyplot as plt
    from meerschaum.utils.packages import import_pandas
    import numpy as np
    pd = import_pandas()
    print(run)
    for scenario, radar_data in scenarios_radar_data.items():
        radar_df = pd.DataFrame(radar_data)
        print(radar_df)
        input()

        for factor in factors:
            max_val = df[factor].max()
            min_val = df[factor].min()
            val_range = max_val - min_val
            df[factor + '_Adj'] = df[factor].apply(
                lambda x: (((x - min_val) * new_range) / val_range) + new_min
            )

        labels = ['cumulative_volume', 'daily_runtime', 'error_rate']
        pt = pd.pivot_table(radar_df, values='number', index=['metric'], columns=['method'])
        fig = plt.figure()
        ax = fig.add_subplot(111, projection="polar")
        theta = np.arange(len(pt))/float(len(pt))*2.*np.pi
        lines = []
        for i, method in enumerate(pt):
            l, = ax.plot(theta, pt[method], color="C" + str(i + 1), marker="o", label=method)
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
        plt.legend()
        plt.title('Foo')
        plt.show()

        print(radar_df)
        input()



if __name__ == "__main__":
    sys.exit(main(sys.argv))
