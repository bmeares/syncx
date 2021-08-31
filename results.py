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
    pd = import_pandas()
    results_dir_path = (
        (pathlib.Path.home() / 'syncx_results') if len(argv) == 1
        else pathlib.Path(argv[1])
    )
    if not results_dir_path.exists():
        print('Usage: python results.py {results directory}', file=sys.stderr)
        return 1
    methods = [
        'naive', 'simple', 'simple-backtrack', 'simple-slow-id', 'append', 'join',
        'simple-monthly-flush', 'rowcount', 'unbounded-dynamic-iterative-simple',
        'unbounded-static-iterative-simple', 'bounded-dynamic-iterative-simple',
        'bounded-static-iterative-simple', 'unbounded-dynamic-iterative-cpi',
        'unbounded-static-iterative-cpi', 'bounded-dynamic-iterative-cpi',
        'bounded-static-iterative-cpi'
    ]
    generic_dtypes = {'Datetime': 'datetime64[ns]', 'Month': 'datetime64[ns]'}
    datasets_methods_dtypes = {
        'cumulative_volume': int,
        'daily_runtime': float,
        #  'daily_volume': int,
        'errors': int,
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
        if run.startswith('.') or not os.path.isdir(run):
            continue
        run_dir_path = pathlib.Path(run)
        for dataset, dtypes in datasets_dtypes.items():

            scenarios_dfs = []
            totals_dfs = []

            scenarios_dir_path = run_dir_path / 'scenarios'
            for scenario in os.listdir(scenarios_dir_path):
                scenario_path = scenarios_dir_path / scenario
                if scenario.startswith('.') or not os.path.isdir(scenario_path):
                    continue
                csv_file_path = scenario_path / 'csv' / (scenario + '_' + dataset + '.csv')
                _df = pd.read_csv(csv_file_path, index_col=0)
                df = _df.astype({col: val for col, val in dtypes.items() if col in _df})
                scenarios_dfs.append(df)
                totals_dfs.append(pd.DataFrame({
                    col: {scenario: (
                        df[col].max() if 'daily' not in dataset
                        else df[col].sum()
                    )}
                    for col in df if col not in generic_dtypes
                }))

            all_totals_df = pd.concat(totals_dfs)
            all_totals_df.index.name = 'Scenario'
            existing_cols = all_totals_df.columns
            all_totals_df['Scenario'] = all_totals_df.index
            all_totals_df = all_totals_df[['Scenario'] + list(existing_cols)]
            
            print(all_totals_df.to_latex(
                multicolumn=True, multirow=True, longtable=False, index_names=False, index=False,
                float_format="%.2f",
                caption=(
                    'Total ' + readable_dataset_names[dataset] + ' for Methods '
                    + items_str(list(existing_cols)))
                )
            )
            input()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
