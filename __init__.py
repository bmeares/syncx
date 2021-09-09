#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement experimental syncing methods.
"""
from __future__ import annotations
from meerschaum import Pipe
from meerschaum.plugins import add_plugin_argument, make_action
from meerschaum.utils.typing import Optional, Any, List, SuccessTuple
from collections import namedtuple
ErrorRow = namedtuple('ErrorRow', ('scenario', 'method', 'error'))

__version__ = '0.1.2'
required = [
    'numpy', 'prime-sieve', 'dateutil', 'galois', 'matplotlib', 'duckdb',
]

add_plugin_argument(
    '--sync-methods', nargs='+', help=(
        "Sync methods to use. Defaults to ['join-new-ids']."
    )
)
add_plugin_argument(
    '--backtrack-minutes', type=int, help=(
        "How many minutes to backtrack when fetching new data."
    )
)
add_plugin_argument(
    '--source', help="Connector keys to the source database (e.g. 'sql:main')"
)
add_plugin_argument(
    '--target', help="Connector keys to the target database (e.g. 'sql:main')"
)
add_plugin_argument(
    '--iterations', help="How many iterations to run per scenario and method (results will be averaged).",
    type=int,
)
@make_action
def scenarios(
        action: Optional[List[str]] = None,
        source: Optional[str] = 'sql:main',
        target: Optional[str] = 'sql:main',
        sync_methods: Optional[List[str]] = None,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        iterations: Optional[int] = None,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Run synchronization scenario simulations.
    """
    from .scenarios import (
        init_scenarios, ITERATIONS_PER_SCENARIO_FM, SIMULATION_BEGIN, SIMULATION_INTERVAL
    )
    from .methods import fetch_methods as fetch_methods_dict, sync_methods as sync_methods_dict
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.warnings import info
    from meerschaum.utils.packages import import_pandas
    from meerschaum.config._paths import PLUGINS_RESOURCES_PATH
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import duckdb
    source_connector = parse_instance_keys(source)
    target_connector = parse_instance_keys(target)
    _scenarios = init_scenarios(source_connector, target_connector)

    usage = "Usage: scenarios ['" + "', '".join(_scenarios.keys()) + "']"
    if (
        begin is not None and end is not None
        and begin.month == end.month and begin.year == end.year
    ):
        return False, "The interval needs to span across at least two months."

    run_scenarios = action
    if not run_scenarios:
        run_scenarios = _scenarios.keys()

    run_sync_methods = sync_methods if sync_methods else list(fetch_methods_dict.keys()) + list(sync_methods_dict.keys())

    for scenario in run_scenarios:
        if scenario not in _scenarios:
            return False, usage
    
    plugin_path = PLUGINS_RESOURCES_PATH / 'syncx'
    scenarios_path = plugin_path / 'scenarios'
    scenarios_path.mkdir(parents=True, exist_ok=True)

    pd = import_pandas()
    for scenario_name in run_scenarios:
        scenario_path = scenarios_path / scenario_name
        scenario_path.mkdir(parents=True, exist_ok=True)
        figures_path = scenario_path / 'figures'
        csv_path = scenario_path / 'csv'
        figures_path.mkdir(parents=True, exist_ok=True)
        csv_path.mkdir(parents=True, exist_ok=True)

        info(f"Testing scenario '{scenario_name}'...")

        drt_methods_dfs, rt_methods_dfs, er_methods_dfs, rer_methods_dfs, vl_methods_dfs, dvl_methods_dfs = [], [], [], [], [], []
        for sm in run_sync_methods:
            info(f"Testing sync method '{sm}'...")
            combo_name = scenario_name + '_' + sm
            runtimes_dfs = []
            averages_dfs = []
            errors_dfs = []
            rel_errors_dfs = []
            cumulative_volumes_dfs = []
            daily_volumes_dfs = []
            for i in range(ITERATIONS_PER_SCENARIO_FM if iterations is None else iterations):
                info(f"Iteration #{i + 1} for scenario '{scenario_name}' with sync method '{sm}'.")
                runtimes_data, errors_data, cumulative_volumes_data, daily_volumes_data = (
                    _scenarios[scenario_name].start(sm, begin=begin, end=end, debug=debug)
                )
                runtimes_df = pd.DataFrame(runtimes_data)
                errors_df = pd.DataFrame(errors_data)[['Datetime', 'Errors']]
                rel_errors_df = pd.DataFrame(errors_data)[['Datetime', 'Percent']]
                cumulative_volumes_df = pd.DataFrame(cumulative_volumes_data)
                daily_volumes_df = pd.DataFrame(daily_volumes_data)
                runtimes_df[sm] = runtimes_df['Runtime']
                runtimes_dfs.append(runtimes_df[['Datetime', sm]])
                averages_dfs.append(
                    duckdb.query(
                        f"""
                        SELECT DATE_TRUNC('month', "Datetime"::DATE) AS 'Month', AVG(Runtime) AS '{sm}'
                        FROM runtimes_df
                        GROUP BY DATE_TRUNC('month', "Datetime"::DATE)
                        """
                    ).to_df()
                )
                errors_df[sm] = errors_df['Errors']
                errors_dfs.append(errors_df[['Datetime', sm]])
                rel_errors_df[sm] = rel_errors_df['Percent']
                rel_errors_dfs.append(rel_errors_df[['Datetime', sm]])
                cumulative_volumes_df[sm] = cumulative_volumes_df['Rows']
                cumulative_volumes_dfs.append(cumulative_volumes_df[['Datetime', sm]])
                daily_volumes_df[sm] = daily_volumes_df['Rows']
                daily_volumes_dfs.append(daily_volumes_df[['Datetime', sm]])

            drt_methods_dfs.append(pd.concat(runtimes_dfs).groupby(by='Datetime', as_index=False).mean())
            rt_methods_dfs.append(pd.concat(averages_dfs).groupby(by='Month', as_index=False).mean())
            er_methods_dfs.append(pd.concat(errors_dfs).groupby(by='Datetime', as_index=False).mean())
            rer_methods_dfs.append(pd.concat(rel_errors_dfs).groupby(by='Datetime', as_index=False).mean())
            vl_methods_dfs.append(pd.concat(cumulative_volumes_dfs).groupby(by='Datetime', as_index=False).mean())
            dvl_methods_dfs.append(pd.concat(daily_volumes_dfs).groupby(by='Datetime', as_index=False).mean())

        ### We've tested all of the fetch methods for this scenario
        ### and now have a list of the average dataframes.
        drt_figure_df = drt_methods_dfs[0]
        for _df in drt_methods_dfs[1:]:
            drt_figure_df = drt_figure_df.merge(_df)

        rt_figure_df = rt_methods_dfs[0]
        for _df in rt_methods_dfs[1:]:
            rt_figure_df = rt_figure_df.merge(_df)

        er_figure_df = er_methods_dfs[0]
        for _df in er_methods_dfs[1:]:
            er_figure_df = er_figure_df.merge(_df)

        rer_figure_df = rer_methods_dfs[0]
        for _df in rer_methods_dfs[1:]:
            rer_figure_df = rer_figure_df.merge(_df)

        vl_figure_df = vl_methods_dfs[0]
        for _df in vl_methods_dfs[1:]:
            vl_figure_df = vl_figure_df.merge(_df)

        dvl_figure_df = dvl_methods_dfs[0]
        for _df in dvl_methods_dfs[1:]:
            dvl_figure_df = dvl_figure_df.merge(_df)

        max_drt = max(drt_figure_df[run_sync_methods].max())
        max_rt = max(rt_figure_df[run_sync_methods].max())
        max_er = max(er_figure_df[run_sync_methods].max())
        min_rer = max(min(rer_figure_df[run_sync_methods].min()) - 10, 0)
        max_rer = min(max(rer_figure_df[run_sync_methods].max()), 100)
        max_vl = max(vl_figure_df[run_sync_methods].max())
        max_dvl = max(dvl_figure_df[run_sync_methods].max())

        ### Create one figure per scenario with all of the methods.
        drt_ax = drt_figure_df.plot(x='Datetime')
        drt_ax.set_ylim([0.0, max_drt + 0.1])
        plt.ylabel("Seconds")
        drt_ax.set_title(f"Daily Runtimes of Scenario '{scenario_name}'")
        drt_figure_df.to_csv(csv_path / (scenario_name + '_daily_runtime.csv'))
        plt.savefig(figures_path / (scenario_name + '_daily_runtime.png'), bbox_inches="tight")

        rt_ax = rt_figure_df.plot(x='Month')
        rt_ax.set_ylim([0.0, max_rt + 0.1])
        plt.ylabel("Seconds")
        rt_ax.set_title(f"Monthly Average Runtimes of Scenario '{scenario_name}'")
        rt_figure_df.to_csv(csv_path / (scenario_name + '_monthly_runtime.csv'))
        plt.savefig(figures_path / (scenario_name + '_monthly_runtime.png'), bbox_inches="tight")

        er_ax = er_figure_df.plot(x='Datetime', kind='line')
        er_ax.set_ylim([0.0, max_er + 10])
        er_ax.xaxis.set_major_locator(mdates.MonthLocator())
        er_ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
        plt.ylabel("Missing Rows")
        er_ax.set_title(f"Cumulative Errors of Scenario '{scenario_name}'")
        er_figure_df.to_csv(csv_path / (scenario_name + '_errors.csv'))
        plt.savefig(figures_path / (scenario_name + '_errors.png'), bbox_inches="tight")

        rer_ax = rer_figure_df.plot(x='Datetime', kind='line')
        rer_ax.set_ylim([min_rer, max_rer])
        rer_ax.xaxis.set_major_locator(mdates.MonthLocator())
        rer_ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
        plt.ylabel("Accuracy Percentage")
        rer_ax.set_title(f"Running Accuracy Rate of Scenario '{scenario_name}'")
        rer_figure_df.to_csv(csv_path / (scenario_name + '_error_rate.csv'))
        plt.savefig(figures_path / (scenario_name + '_error_rate.png'), bbox_inches="tight")


        vl_ax = vl_figure_df.plot(x='Datetime', kind='line')
        vl_ax.set_ylim([0, max_vl + 10])
        vl_ax.xaxis.set_major_locator(mdates.MonthLocator())
        vl_ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
        plt.ylabel("Rows Transferred")
        vl_ax.set_title(f"Cumulative Fetched Row Volume of Scenario '{scenario_name}'")
        vl_figure_df.to_csv(csv_path / (scenario_name + '_cumulative_volume.csv'))
        plt.savefig(figures_path / (scenario_name + '_cumulative_volume.png'), bbox_inches="tight")

        dvl_ax = dvl_figure_df.plot(x='Datetime', kind='line')
        dvl_ax.set_ylim([0, max_dvl + 10])
        dvl_ax.xaxis.set_major_locator(mdates.MonthLocator())
        dvl_ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
        plt.ylabel("Rows Transferred")
        dvl_ax.set_title(f"Daily Fetched Row Volume of Scenario '{scenario_name}'")
        dvl_figure_df.to_csv(csv_path / (scenario_name + '_daily_volume.csv'))
        plt.savefig(figures_path / (scenario_name + '_daily_volume.png'), bbox_inches="tight")


    return True, "Success"

def sync(
        pipe: Pipe,
        sync_method: str = 'join-new-ids',
        backtrack_minutes: Optional[int] = None,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    :param sync_method:
        The method used to request new data based on the cache context.
        Options:

            - `naive`
                Select all data.

            - `simple`
                Select data newer than the most recent datetime.
                
            - `simple-backtrack`
                Select data newer than the most recent datetime, minus a defined backtrack interval.
            
            - `simple-slow-id`
                Select data newer than the slowest ID's most recent datetime.

            - `append`
                Select data newer than each ID's most recent datetimes and append the results.

            - `append-new-ids`
                Same as `append` with a subquery to catch new IDs.

            - `join`
                Build a `sync_times` table from the cache, then generate
                a subquery to reproduce the table and LEFT OUTER JOIN the table
                on the remote table.

            - `join-new-ids`
                Same as `join` with an `OR` clause to catch new IDs.
    """
    from meerschaum import Pipe
    from meerschaum.utils.warnings import warn
    from meerschaum.connectors.parse import parse_connector_keys
    definition = pipe.parameters.get('fetch', {}).get('definition', None)
    if definition is None:
        warn(f"No definition for pipe '{pipe}'.")
        return None

    conn_keys = pipe.parameters.get('fetch', {}).get('connector', 'sql')
    conn = parse_connector_keys(conn_keys)
    _pipe = Pipe(instance=pipe.instance_connector, **pipe.meta)
    _pipe._connector = conn

    if backtrack_minutes is not None:
        pipe.parameters['backtrack_minutes'] = backtrack_minutes

    from .scenarios import empty_df
    from .methods import fetch_methods, sync_methods
    if sync_method not in sync_methods and sync_method not in fetch_methods:
        warn(f"Invalid sync method '{sync_method}'.")
        return None

    ### If not optimizations may be made, perform a naive sync.
    if not pipe.exists(debug=debug):
        return sync_methods['naive'](_pipe, with_extras=with_extras, debug=debug)

    if sync_method in fetch_methods:
        fetched_df = fetch_methods[sync_method](
            _pipe,
            debug = debug
        )
        filtered_df = (
            _pipe.filter_existing(fetched_df, debug=debug)
            if fetched_df is not None else empty_df()
        )
        success_tuple = _pipe.sync(filtered_df, check_existing=False, debug=debug)
        if with_extras:
            return success_tuple, filtered_df, fetched_df
        return success_tuple

    return sync_methods[sync_method](
        _pipe,
        with_extras = with_extras,
        debug = debug,
        **kw
    )
