#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement experimental syncing methods.
"""

from meerschaum import Pipe
from meerschaum.actions.arguments import add_plugin_argument
from meerschaum.utils.typing import Optional, Any

__version__ = '0.0.1'
required = [
    'numpy', 'prime-sieve', 'dateutil', 'galois',
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
    from meerschaum.utils.warnings import warn
    from meerschaum.connectors.parse import parse_connector_keys
    definition = pipe.parameters.get('fetch', {}).get('definition', None)
    if definition is None:
        warn(f"No definition for pipe '{pipe}'.")
        return None

    conn_keys = pipe.parameters.get('fetch', {}).get('connector', 'sql')
    conn = parse_connector_keys(conn_keys)
    pipe._connector = conn

    if backtrack_minutes is not None:
        pipe.parameters['backtrack_minutes'] = backtrack_minutes

    from .methods import fetch_methods
    if fetch_method not in fetch_methods:
        warn(f"Invalid fetch method '{fetch_method}'.")
        return None

    return fetch_methods[fetch_method](pipe, debug=debug, new_ids=('new-ids' in fetch_method), **kw)
