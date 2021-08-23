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

__version__ = '0.0.2'
required = [
    'numpy', 'prime-sieve', 'dateutil', 'galois', 'matplotlib', 'duckdb',
]

add_plugin_argument(
    '--fetch-method', help=(
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
        fetch_method: Optional[str] = None,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Run synchronization scenario simulations.
    """
    from .scenarios import init_scenarios
    from .methods import fetch_methods
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.warnings import info
    from meerschaum.utils.packages import import_pandas
    import matplotlib.pyplot as plt
    import duckdb
    source_connector = parse_instance_keys(source)
    target_connector = parse_instance_keys(target)
    _scenarios = init_scenarios(source_connector, target_connector)

    usage = "Usage: scenarios ['" + "', '".join(_scenarios.keys()) + "']"

    run_scenarios = action
    if not run_scenarios:
        run_scenarios = _scenarios.keys()

    run_fetch_methods = [fetch_method] if fetch_method is not None else fetch_methods.keys()

    for scenario in run_scenarios:
        if scenario not in _scenarios:
            return False, usage
    

    pd = import_pandas()
    for _fetch_method in run_fetch_methods:
        info(f"Testing fetch method '{_fetch_method}'...")

        for scenario_name in run_scenarios:
            runtimes_data, errors = _scenarios[scenario_name].start(_fetch_method, debug=debug)
            runtimes_df = pd.DataFrame(runtimes_data)
            avg_runtimes_df = duckdb.query(
                """
                SELECT DATE_TRUNC('month', Datetime) AS 'Month', AVG(Runtime) AS 'Average Runtime'
                FROM runtimes_df
                GROUP BY DATE_TRUNC('month', Datetime)
                """
            ).to_df()
            avg_runtimes_df.plot(x='Month', y='Average Runtime')
            plt.show()

    return True, "Success"

def fetch(
        pipe: Pipe,
        fetch_method: str = 'join-new-ids',
        backtrack_minutes: Optional[int] = None,
        debug: bool = False,
        **kw
    ):
    """
    :param fetch_method:
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

    from .methods import fetch_methods
    if fetch_method not in fetch_methods:
        warn(f"Invalid fetch method '{fetch_method}'.")
        return None

    if not pipe.exists(debug=debug):
        return fetch_methods['naive'](_pipe, debug=debug)
    return fetch_methods[fetch_method](
        _pipe,
        debug = debug,
        new_ids = ('new-ids' in fetch_method),
        **kw
    )
