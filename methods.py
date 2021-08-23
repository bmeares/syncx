#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the syncing methods.
"""
from __future__ import annotations

from meerschaum import Pipe
from meerschaum.utils.typing import Any, Union, Optional, SuccessTuple
from meerschaum.connectors.sql._tools import sql_item_name, table_exists, dateadd_str

#######################################
#                                     #
#            Fetch methods            #
#                                     #
#######################################

def _naive_fetch(pipe, debug: bool=False, **kw) -> Union['pd.DataFrame', None]:
    return pipe.connector.read(pipe.parameters['fetch']['definition'], debug=debug)

def _simple_fetch(pipe, debug: bool=False, **kw) -> Union['pd.DataFrame', None]:
    pipe.parameters['backtrack_minutes'] = 0
    return pipe.fetch(debug=debug, **kw)

def _simple_backtrack_fetch(pipe, debug: bool=False, **kw) -> Union['pd.DataFrame', None]:
    return pipe.fetch(debug=debug, **kw)

def _simple_slow_id_fetch(
        pipe,
        debug: bool=False,
        **kw
    ) -> Union['pd.DataFrame', None]:
    """
    Fetch data newer than the slowest ID's datetime value.
    """
    pipe_name = sql_item_name(str(pipe), pipe.connector.flavor)
    dt_name = sql_item_name(pipe.columns.get('datetime'), pipe.connector.flavor)
    slow_id_st_query = f"""
    WITH sync_times AS (
        SELECT id, MAX({dt_name}) AS sync_time
        FROM {pipe_name}
        GROUP BY id
    ) SELECT MIN(sync_time) AS st
    FROM sync_times
    """
    st = pipe.connector.value(slow_id_st_query)
    kw['begin'] = (st if kw.get('begin', None) is None else kw.get('begin'))
    return pipe.fetch(**kw)

def _append_fetch(
        pipe,
        debug: bool=False,
        new_ids: bool = False,
        **kw
    ) -> Union['pd.DataFrame', None]:
    """
    Append together individual simple syncs per ID via UNION ALL.
    """
    pipe_name = sql_item_name(str(pipe), pipe.connector.flavor)
    id_name = sql_item_name(pipe.columns['id'], pipe.connector.flavor)
    dt_name = sql_item_name(pipe.columns['datetime'], pipe.connector.flavor)
    cols_types = pipe.get_columns_types(debug=debug)
    sync_times_query = f"""
    SELECT {id_name}, MAX({dt_name}) AS sync_time
    FROM {pipe_name}
    GROUP BY {id_name}
    """
    definition = pipe.parameters['fetch']['definition']
    sync_times = pipe.connector.read(sync_times_query, debug=debug)
    if len(sync_times) == 0:
        return _naive_fetch(pipe, debug=debug, **kw)

    query = f"WITH definition AS ({definition})\n"
    for _id, _st in sync_times.itertuples(index=False):
        query += (
            "SELECT * FROM definition "
            + f"WHERE {id_name} = CAST('{_id}' AS {cols_types[pipe.columns['id']]})\n"
            + f"  AND {dt_name} > " + dateadd_str(
                flavor = pipe.connector.flavor,
                datepart = 'minute',
                number = pipe.parameters.get('backtrack_minutes', 0),
                begin = _st
            ) + "\n"
            + "UNION ALL\n"
        )
    query = (
        query[:(-1 * len('UNION ALL\n'))] if not new_ids
        else (
            query + (
                "SELECT * FROM definition\n"
                + f"WHERE {id_name} NOT IN ("
                + ', '.join(
                    [
                        f"CAST('{_id}' AS {cols_types[pipe.columns['id']]})"
                        for _id in sync_times[pipe.columns['id']]
                    ]
                ) + ")"
            )
        )
    )
    return pipe.connector.read(query, debug=debug)

def _join_fetch(
        pipe,
        debug: bool=False,
        new_ids: bool=False,
        **kw
    ) -> Union['pd.DataFrame', None]:
    pipe_name = sql_item_name(str(pipe), pipe.connector.flavor)
    sync_times_table = ('#' if pipe.connector.flavor == 'mssql' else '') + str(pipe) + "_sync_times"
    sync_times_name = sql_item_name(sync_times_table, pipe.connector.flavor)
    id_name = sql_item_name(pipe.columns['id'], pipe.connector.flavor)
    dt_name = sql_item_name(pipe.columns['datetime'], pipe.connector.flavor)
    cols_types = pipe.get_columns_types(debug=debug)
    sync_times_query = f"""
    SELECT {id_name}, MAX({dt_name}) AS {dt_name}
    FROM {pipe_name}
    GROUP BY {id_name}
    """
    sync_times = pipe.connector.read(sync_times_query, debug=debug)
    if len(sync_times) == 0:
        return _naive_fetch(pipe, debug=debug)
    #  if table_exists(sync_times_table, pipe.connector, debug=debug):
        #  pipe.connector.exec(f"DROP TABLE {sync_times_name}", silent=True, debug=debug)
    #  create_temp_table_query = (
        #  f"CREATE TEMP TABLE {sync_times_name} ("
        #  + id_name + " " + cols_types[pipe.columns['id']] + ", "
        #  + dt_name + " " + cols_types[pipe.columns['datetime']] + ")"
    #  )

    _sync_times_q = ""
    _created_temp_table = False
    #  try:
        #  if pipe.connector.exec(
            #  create_temp_table_query, debug=debug, silent=True
        #  ) is not None:
            #  pipe.connector.to_sql(sync_times, name=sync_times_table, if_exists='append')
            #  _created_temp_table = True
    #  except Exception as e:
        #  _created_temp_table = False

    if not _created_temp_table:
        _sync_times_q = f",\n{sync_times_name} AS ("
        for _id, _st in sync_times.itertuples(index=False):
            _sync_times_q += (
                f"SELECT CAST('{_id}' AS " + cols_types[pipe.columns['id']] + f") AS {id_name}, "
                + dateadd_str(
                    flavor=pipe.connector.flavor,
                    begin=_st,
                    datepart='minute',
                    number=pipe.parameters.get('backtrack_minutes', 0)
                ) + " AS " + dt_name + "\nUNION ALL\n"
            )
        _sync_times_q = _sync_times_q[:(-1 * len('UNION ALL\n'))] + ")"

    definition = pipe.parameters['fetch']['definition']
    query = f"""
    WITH definition AS ({definition}){_sync_times_q}
    SELECT d.*
    FROM definition AS d
    LEFT OUTER JOIN {sync_times_name} AS st
      ON st.{id_name} = d.{id_name}
    WHERE d.{dt_name} > st.{dt_name}
    """ + (f"  OR st.{id_name} IS NULL" if new_ids else "")
    return pipe.connector.read(query, debug=debug)


######################################
#                                    #
#            Sync methods            #
#                                    #
######################################


def _iterative_simple_sync(
        pipe: Pipe,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Iterate across a pipe's interval and perform a simple sync for each chunk.
    """
    rt0 = pipe.get_sync_time(newest=True, debug=debug)
    rt1 = pipe.get_sync_time(newest=False, debug=debug)

fetch_methods = {
    'naive': _naive_fetch,
    'simple': _simple_fetch,
    'simple-backtrack': _simple_backtrack_fetch,
    'simple-slow-id': _simple_slow_id_fetch,
    'append': _append_fetch,
    'append-new-ids': _append_fetch,
    'join': _join_fetch,
    'join-new-ids': _join_fetch,
}
sync_methods = {
    'iterative-simple-sync': _iterative_simple_sync,
}
