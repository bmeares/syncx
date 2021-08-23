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

__version__ = '0.0.5'
required = [
    'numpy', 'prime-sieve', 'dateutil', 'galois', 'matplotlib', 'duckdb',
]

add_plugin_argument(
    '--sync-method', help=(
        "Type of fetch method to use. Defaults to 'join-new-ids'."
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

@make_action
def scenarios(
        action: Optional[List[str]] = None,
        source: Optional[str] = 'sql:main',
        target: Optional[str] = 'sql:main',
        sync_method: Optional[str] = None,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Run synchronization scenario simulations.
    """
    from .scenarios import init_scenarios, ITERATIONS_PER_SCENARIO_FM
    from .methods import fetch_methods, sync_methods
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.warnings import info
    from meerschaum.utils.packages import import_pandas
    from meerschaum.config._paths import PLUGINS_RESOURCES_PATH
    import matplotlib.pyplot as plt
    import duckdb
    source_connector = parse_instance_keys(source)
    target_connector = parse_instance_keys(target)
    _scenarios = init_scenarios(source_connector, target_connector)

    usage = "Usage: scenarios ['" + "', '".join(_scenarios.keys()) + "']"

    run_scenarios = action
    if not run_scenarios:
        run_scenarios = _scenarios.keys()

    run_sync_methods = [sync_method] if sync_method is not None else list(fetch_methods.keys()) + list(sync_methods.keys())

    for scenario in run_scenarios:
        if scenario not in _scenarios:
            return False, usage
    
    plugin_path = PLUGINS_RESOURCES_PATH / 'syncx'
    figures_path = plugin_path / 'figures'
    csv_path = plugin_path / 'csv'
    figures_path.mkdir(parents=True, exist_ok=True)
    csv_path.mkdir(parents=True, exist_ok=True)

    errors_data = []
    
    pd = import_pandas()
    for scenario_name in run_scenarios:
        info(f"Testing scenario '{scenario_name}'...")

        methods_dfs = []
        for sm in run_sync_methods:
            info(f"Testing sync method '{sm}'...")
            combo_name = scenario_name + '_' + sm
            averages_dfs = []
            for i in range(ITERATIONS_PER_SCENARIO_FM):
                info(f"Iteration #{i + 1} for scenario '{scenario_name}' with fetch method '{_fetch_method}'.")
                runtimes_data, error = _scenarios[scenario_name].start(sm, debug=debug)
                errors_data.append(ErrorRow(scenario_name, sm, error))
                runtimes_df = pd.DataFrame(runtimes_data)
                averages_dfs.append(
                    duckdb.query(
                        f"""
                        SELECT DATE_TRUNC('month', Datetime) AS 'Month', AVG(Runtime) AS '{_fetch_method}'
                        FROM runtimes_df
                        GROUP BY DATE_TRUNC('month', Datetime)
                        """
                    ).to_df()
                )
            methods_dfs.append(pd.concat(averages_dfs).groupby(by='Month', as_index=False).mean())

        ### We've tested all of the fetch methods for this scenario
        ### and now have a list of the average dataframes.
        figure_df = methods_dfs[0]
        for _df in methods_dfs[1:]:
            figure_df = figure_df.merge(_df)

        max_rt = max(figure_df[run_sync_methods].max())

        ### Create one figure per scenario with all of the methods.
        ax = figure_df.plot(x='Month')
        ax.set_ylim([0.0, max_rt + 0.05])
        ax.set_title(f"Average Runtimes of Scenario '{scenario_name}'")
        figure_df.to_csv(csv_path / (scenario_name + '.csv'))
        plt.savefig(figures_path / (scenario_name + '.png'), bbox_inches="tight")

    ### Finally, save the combinations of errors per scenario and method.
    errors_df = pd.DataFrame(errors_data).set_index('scenario')
    errors_df.to_csv(csv_path / 'errors.csv')

    return True, "Success"

def sync(
        pipe: Pipe,
        sync_method: str = 'join-new-ids',
        backtrack_minutes: Optional[int] = None,
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
    _pipe = Pipe(**pipe.meta)
    _pipe._connector = conn

    if backtrack_minutes is not None:
        pipe.parameters['backtrack_minutes'] = backtrack_minutes

    from .methods import fetch_methods, sync_methods
    if sync_method not in sync_methods and sync_method not in fetch_methods:
        warn(f"Invalid sync method '{sync_method}'.")
        return None

    ### If not optimizations may be made, perform a naive sync.
    if not pipe.exists(debug=debug):
        return pipe.sync(
            fetch_methods['naive'](_pipe, debug=debug)
        )

    return (
        pipe.sync(
            fetch_methods[sync_method](
                _pipe,
                new_ids = ('new-ids' in sync_method),
                debug = debug,
            )
        ) if sync_method in fetch_methods
        else sync_methods[sync_method](
            _pipe,
            new_ids = ('new-ids' in sync_method),
            debug = debug,
        )
    )
