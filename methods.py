#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the syncing methods.
"""
from __future__ import annotations

import datetime
from meerschaum import Pipe
from meerschaum.utils.typing import Any, Union, Optional, SuccessTuple, Callable, Tuple
from meerschaum.connectors.sql._tools import sql_item_name, table_exists, dateadd_str

CHUNKSIZE = 1000

#######################################
#                                     #
#            Fetch methods            #
#                                     #
#######################################

def _naive_fetch(pipe, debug: bool=False, **kw) -> Union['pd.DataFrame', None]:
    """
    Return the entire source table.
    """
    return pipe.connector.read(pipe.parameters['fetch']['definition'], debug=debug)

def _simple_fetch(pipe, debug: bool=False, **kw) -> Union['pd.DataFrame', None]:
    """
    Fetch data from the source connector. Forces `backtrack_minutes` to be zero.
    """
    old_btm = pipe.parameters.get('fetch', {}).get('backtrack_minutes', None)
    pipe.parameters['fetch']['backtrack_minutes'] = 0
    df = pipe.fetch(debug=debug, begin=pipe.get_sync_time(debug=debug))
    if old_btm is None:
        del pipe.parameters['fetch']['backtrack_minutes']
    return df

def _simple_backtrack_fetch(
        pipe,
        bti: Optional[datetime.timedelta] = None,
        debug: bool = False,
        **kw
    ) -> Union['pd.DataFrame', None]:
    sync_time = pipe.get_sync_time(debug=debug)
    bti = DEFAULT_BTI_INIT if bti is None else bti
    begin = (sync_time - bti) if sync_time is not None else None
    return pipe.fetch(begin=begin, debug=debug, **kw)

def _simple_slow_id_fetch(
        pipe,
        debug: bool = False,
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
    return pipe.fetch(debug=debug, **kw)

def _append_fetch(
        pipe,
        debug: bool = False,
        new_ids: bool = True,
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
        debug: bool = False,
        new_ids: bool = True,
        **kw
    ) -> Union['pd.DataFrame', None]:
    pipe_name = sql_item_name(str(pipe), pipe.connector.flavor)
    sync_times_table = ('#' if pipe.connector.flavor == 'mssql' else '') + str(pipe) + "_sync_times"
    sync_times_name = sql_item_name(sync_times_table, pipe.connector.flavor)
    sync_times_values_name = sql_item_name(sync_times_table + '_values', pipe.connector.flavor)
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
    _sync_times_q = ""
    _created_temp_table = False
    if not _created_temp_table:
        _sync_times_q = f",\n{sync_times_name} AS (\nSELECT * FROM (\nVALUES\n"
        for _id, _st in sync_times.itertuples(index=False):
            _sync_times_q += (
                f"(CAST('{_id}' AS " + cols_types[pipe.columns['id']] + f"), "
                + dateadd_str(
                    flavor=pipe.connector.flavor,
                    begin=_st,
                    datepart='minute',
                    number=pipe.parameters.get('backtrack_minutes', 0)
                ) + "),"
            )
        _sync_times_q = (
            _sync_times_q[:-1] + f"\n) AS t({id_name}, {dt_name})\n) "
        )

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


def _cpi_fetch(
        pipe: Pipe,
        with_extras: bool = False,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        debug: bool = False,
        **kw
    ) -> 'pd.DataFrame':
    """
    Determine the difference between the source and target via CPISync.
    Only works when row datetimes are unique and datetimes only have 1-second resolution.
    It's possible to increase the resolution if a Pipe has a set frequency,
    but for now I'm sticking with a maximum of 1-second.
    """
    from .cpi import _choose_prime
    from meerschaum.utils.misc import round_time
    from meerschaum.connectors.sql.tools import sql_item_name
    from meerschaum.utils.packages import import_pandas
    import galois
    import numpy as np
    pd = import_pandas()
    CPI_THRESHOLD = 1000

    source_dt_name = sql_item_name(pipe.columns['datetime'], pipe.connector.flavor)
    target_dt_name = sql_item_name(pipe.columns['datetime'], pipe.instance_connector.flavor)
    source_id_name = (
        sql_item_name(pipe.columns['id'], pipe.connector.flavor)
        if 'id' in pipe.columns else None
    )
    target_id_name = (
        sql_item_name(pipe.columns['id'], pipe.instance_connector.flavor)
        if 'id' in pipe.columns else None
    )
    if begin is None:
        begin_query = f"""
        WITH definition AS ({pipe.parameters['fetch']['definition']})
        SELECT MIN({source_dt_name}) AS dt
        FROM definition
        """
        begin = pipe.instance_connector.value(begin_query, debug=debug)

    if end is None:
        end_query = f"""
        WITH definition AS ({pipe.parameters['fetch']['definition']})
        SELECT MAX({source_dt_name}) AS dt
        FROM definition
        """
        end = pipe.instance_connector.value(end_query, debug=debug)

    begin_int = int(begin.replace(tzinfo=datetime.timezone.utc).timestamp())
    end_int = int(end.replace(tzinfo=datetime.timezone.utc).timestamp() - begin_int) + 1

    pipe_name = sql_item_name(str(pipe), pipe.instance_connector.flavor)
    ids_query = f"SELECT DISTINCT {target_id_name} FROM {pipe_name}"
    target_ids = (
        list(pipe.instance_connector.read(ids_query, debug=debug)[pipe.columns['id']])
        if target_id_name is not None else [None]
    )

    ### TODO get new IDs from source
    #  get_new_ids = 

    def _per_id(_id):
        source_rowcount = pipe.connector.get_pipe_rowcount(
            pipe, remote=True, begin=begin, end=end, debug=debug, params={pipe.columns['id']: _id},
        )
        target_rowcount = pipe.instance_connector.get_pipe_rowcount(
            pipe, begin=begin, end=end, debug=debug, params={pipe.columns['id']: _id}
        )

        if source_rowcount == target_rowcount:
            return None           


        m = int(source_rowcount - target_rowcount) * 3
        if m > CPI_THRESHOLD or target_rowcount == 0:
            fetched_df = pipe.fetch(begin=begin, end=end, params={pipe.columns['id']: _id}, debug=debug)
            return pipe.filter_existing(fetched_df, debug=debug)

        Zs = list(range(-1, (-1 * (m + 1)), -1))
        prime = _choose_prime(end_int)
        GF = galois.GF(prime)
        fZs = np.negative(GF([abs(Z) for Z in Zs]))

        _source_mod = ('%' if pipe.connector.flavor == 'duckdb' else '%%')
        _target_mod = ('%' if pipe.instance_connector.flavor == 'duckdb' else '%%')
        target_chi_queries = [(f"""
        WITH RECURSIVE t(c) as (
            SELECT ({Z} - (EXTRACT(EPOCH FROM {target_dt_name}) - {begin_int}))::BIGINT
            FROM {pipe_name}
            WHERE {target_dt_name} > '{begin}'::TIMESTAMP AND {target_dt_name} <= '{end}'::TIMESTAMP
        """ + (f" AND {target_id_name} = '{_id}'" if _id is not None else '') +
        """
        ), r(c,n) AS (
            SELECT t.c, row_number() OVER () FROM t
        ), p(c,n) AS (
            SELECT c, n FROM r WHERE n = 1
            UNION ALL
            SELECT (r.c * p.c) """ + _target_mod + f""" {prime}, r.n
            FROM p JOIN r ON p.n + 1 = r.n
        )
        SELECT c
        FROM p WHERE n = (
            SELECT max(n) FROM p
        ) 
        """) for Z in Zs]

        source_chi_queries = [(f"""
        WITH RECURSIVE t(c) as (
            SELECT ({Z} - (EXTRACT(EPOCH FROM {source_dt_name}) - {begin_int}))::BIGINT
            FROM (
                SELECT {source_dt_name}
                FROM ({pipe.parameters['fetch']['definition']}) AS definition
                WHERE {source_dt_name} > '{begin}'::TIMESTAMP AND {source_dt_name} <= '{end}'::TIMESTAMP
            """ + (f" AND {target_id_name} = '{_id}'" if _id is not None else '') +
        """
            ) AS src
        ), r(c,n) AS (
            SELECT t.c, row_number() OVER () FROM t
        ), p(c,n) AS (
            SELECT c, n FROM r WHERE n = 1
            UNION ALL
            SELECT (r.c * p.c) """ + _source_mod + f""" {prime}, r.n
            FROM p JOIN r ON p.n + 1 = r.n
        )
        SELECT c
        FROM p WHERE n = (
            SELECT max(n) FROM p
        ) 
        """) for Z in Zs]
        chis_source = GF([
            pipe.connector.value(query, debug=debug) % prime for query in source_chi_queries]
        )
        chis_target = GF([
            pipe.instance_connector.value(query, debug=debug) % prime
            for query in target_chi_queries]
        )
        ratios = np.divide(chis_source, chis_target)
        polynomial_fZs = GF([[(Z**i) for i in range(m)] for Z in fZs])
        coefficients = np.linalg.solve(
            polynomial_fZs,
            ratios
        )
        poly = galois.Poly(coefficients, order='asc')
        delta = [datetime.datetime.utcfromtimestamp(int(offset) + begin_int) for offset in poly.roots()]
        final_query = f"""
        WITH definition AS (
            {pipe.parameters['fetch']['definition']}
        ) SELECT * FROM definition
        WHERE {source_dt_name} IN ("""
        for dt in delta:
            final_query += f"'{dt}'::TIMESTAMP, "
        final_query = (
            final_query[:-2] + ')' + (f" AND {source_id_name} = '{_id}'" if _id is not None else '')
        )
        return pipe.connector.read(final_query, debug=debug)
    
    fetched_dfs = [_per_id(_id) for _id in target_ids]
    for df in fetched_dfs:
        if df is not None:
            return pd.concat(fetched_dfs).drop_duplicates()
    return None


def _binary_fetch(
        pipe: Pipe,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        debug: bool = False,
        **kw
    ):
    """
    Binary search between begin and end to find missing rows.
    """
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas()
    source_rowcount = pipe.connector.get_pipe_rowcount(
        pipe, remote=True, begin=begin, end=end, debug=debug,
    )
    target_rowcount = pipe.instance_connector.get_pipe_rowcount(
        pipe, begin=begin, end=end, debug=debug,
    )
    if source_rowcount == target_rowcount:
        return None
    if target_rowcount == 0:
        return pipe.fetch(begin=begin, end=end, debug=debug)

    threshold = datetime.timedelta(minutes=15)
    intervals = []
    def find_intervals(_begin, _end):
        _target_rowcount = pipe.instance_connector.get_pipe_rowcount(
            pipe, begin=_begin, end=_end, debug=debug,
        )
        _source_rowcount = pipe.connector.get_pipe_rowcount(
            pipe, remote=True, begin=_begin, end=_end, debug=debug,
        )
        if _target_rowcount == _source_rowcount:
            return
        if _target_rowcount == 0:
            intervals.append((_begin, _end))
            return

        ### Recurse on the first half
        _interval = _end - _begin

        if _interval < threshold:
            intervals.append((_begin, _end))
            return

        find_intervals(_begin, _end - (_interval / 2))
        find_intervals(_begin + (_interval / 2), _end)

    find_intervals(begin, end)
    fetched_dfs = []
    for _begin, _end in intervals:
        fetched_dfs.append(pipe.fetch(begin=_begin, end=_end))
    return pd.concat(fetched_dfs) if fetched_dfs else None


######################################
#                                    #
#            Sync methods            #
#                                    #
######################################

DEFAULT_BTI_INIT = datetime.timedelta(hours=1)
DEFAULT_GROW_BTI_MAX = datetime.timedelta(hours=720)
DEFAULT_GROW_BTI_FACTOR = 1.4
def _default_grow_bti(bti: datetime.timedelta) -> datetime.timedelta:
    """
    Grow the BTI by 40% and cap at 720 hours.
    """
    return min(DEFAULT_GROW_BTI_FACTOR * bti, DEFAULT_GROW_BTI_MAX)

def _naive_sync(
        pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Drop the pipe and completely resync each time.
    """
    pipe.drop(debug=debug)
    fetched_df = _naive_fetch(pipe, debug=debug)
    filtered_df = pipe.filter_existing(fetched_df, debug=debug)
    success_tuple = pipe.sync(
        filtered_df,
        check_existing = False,
        debug = debug,
    )
    if with_extras:
        return success_tuple, filtered_df, fetched_df
    return success_tuple

def _unbounded_dynamic_iterative_simple_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Iterate across a pipe's interval and perform a simple sync for each chunk.

    :param pipe:
        The pipe to sync.

    :param bti:
        The backtrack interval (partition size).
        Defaults to 1 hour.

    :param grow_bti:
        Function to grow `bti` between iterations.
        If `grow_bti` is False, do not increase `bti`.
        Defaults to a 40% increase with a cap of 720 hours.
    """
    return _generic_iterate_sync(pipe, with_extras=with_extras, debug=debug, **kw)

def _unbounded_static_iterative_simple_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Iterate across a pipe's interval and perform a simple sync for each chunk.
    Use a static BTI of 24 hours.
    """
    return _generic_iterate_sync(
        pipe,
        with_extras = with_extras,
        grow_bti = False,
        bti = datetime.timedelta(hours=24),
        debug = debug,
        **kw
    )

def _bounded_dynamic_iterative_simple_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Iterative across the pipe's interval and perform a simple on each interval.
    Stop iterating past 720 hours.
    """
    return _generic_iterate_sync(
        pipe,
        fetch_function = _simple_fetch,
        max_traversal_interval = datetime.timedelta(hours=720),
        with_extras = with_extras,
        debug = debug,
    )

def _bounded_static_iterative_simple_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Iterative across the pipe's interval and perform a simple on each interval.
    Stop iterating past 720 hours.
    """
    return _generic_iterate_sync(
        pipe,
        fetch_function = _simple_fetch,
        max_traversal_interval = datetime.timedelta(hours=720),
        bti = datetime.timedelta(hours=24),
        grow_bti = False,
        with_extras = with_extras,
        debug = debug,
    )

def _generic_iterate_sync(
        pipe: Pipe,
        bti: Optional[datetime.timedelta] = None,
        grow_bti: Optional[Callable[[datetime.timedelta], datetime.timedelta]] = None,
        max_traversal_interval: Optional[datetime.timedelta] = None,
        with_extras: bool = False,
        check_existing: bool = True,
        fetch_function: Callable[
            [Pipe, Optional[datetime], Optional[datetime]],
            'pd.DataFrame'
        ] = _simple_fetch,
        debug: bool = False,
        **kw
    ):
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.misc import filter_unseen_df, round_time
    pd = import_pandas()

    rt0 = pipe.get_sync_time(newest=True, debug=debug)
    if rt0 is None:
        return _naive_sync(
            pipe,
            with_extras = with_extras,
            debug = debug,
        )
    rt1 = (
        pipe.get_sync_time(newest=False, debug=debug) if max_traversal_interval is None
        else rt0 - max_traversal_interval
    )
    if bti is None:
        bti = DEFAULT_BTI_INIT

    if grow_bti is None:
        grow_bti = _default_grow_bti

    new_dfs, fetched_dfs = [], []

    ### First sync rows newer than rt0.
    fetched_df = fetch_function(pipe, begin=rt0, debug=debug)
    if fetched_df is not None:
        filtered_df = pipe.filter_existing(fetched_df, chunksize=CHUNKSIZE, debug=debug) if check_existing else fetched_df
        success_tuple = pipe.sync(
            filtered_df,
            check_existing = False,
            chunksize = CHUNKSIZE,
            debug = debug,
        )
        new_dfs.append(filtered_df)
        fetched_dfs.append(fetched_df)

    et = rt0
    st = rt0 - bti
    while st > rt1:
        ### Perform a simple sync over the interval from st to et.
        fetched_df = fetch_function(pipe, begin=st, end=et, debug=debug)
        if fetched_df is not None:
            filtered_df = pipe.filter_existing(fetched_df, chunksize=CHUNKSIZE, debug=debug) if check_existing else fetched_df
            success_tuple = pipe.sync(
                filtered_df,
                check_existing = False,
                chunksize = CHUNKSIZE,
                debug = debug,
            )
            new_dfs.append(filtered_df)
            fetched_dfs.append(fetched_df)

        ### Move to the next chunk in the past.
        bti = grow_bti(bti) if grow_bti is not False else bti
        et = st
        st = round_time(et - bti)

    ### Finally sync rows older than rt1 (only if a maximum interval is not provided).
    if max_traversal_interval is not None:
        fetched_df = fetch_function(pipe, end=rt1, debug=debug)
        if fetched_df is not None:
            filtered_df = pipe.filter_existing(fetched_df, chunksize=CHUNKSIZE, debug=debug) if check_existing else fetched_df
            success_tuple = pipe.sync(
                filtered_df,
                check_existing = False,
                debug = debug,
            )
            new_dfs.append(filtered_df)
            fetched_dfs.append(fetched_df)
    if with_extras:
        return success_tuple, pd.concat(new_dfs), pd.concat(fetched_dfs)
    return success_tuple

def _simple_monthly_flush_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Perform a simple sync but flush the pipe (naive sync) at the beginning of every month.
    """
    return _generic_monthly_sync(pipe, with_extras=with_extras, debug=debug)

def _simple_monthly_unbounded_dynamic_iterative_cpi_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Perform a simple sync but call an unbounded dynamic iterative CPISync at the beginning of each month.
    """
    return _generic_monthly_sync(
        pipe,
        monthly_sync_function = _unbounded_dynamic_iterative_cpi_sync,
        with_extras = with_extras,
        debug = debug,
    )


def _simple_monthly_unbounded_dynamic_iterative_binary_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Perform a simple sync but call an unbounded dynamic iterative binary search sync at the beginning of each month.
    """
    return _generic_monthly_sync(
        pipe,
        monthly_sync_function = _unbounded_dynamic_iterative_binary_sync,
        with_extras = with_extras,
        debug = debug,
    )


def _generic_monthly_sync(
        pipe: Pipe,
        with_extras: bool = False,
        monthly_sync_function: Callable[[Pipe], pd.DataFrame] = _naive_sync,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Perform a simple sync but call the monthly sync function at the beginning of every month.
    """
    from .scenarios import get_last_month
    last_sync_time = pipe.parameters['fetch'].get('last_sync_time', None)
    if last_sync_time is None:
        naive_result = _naive_sync(pipe, with_extras=with_extras, debug=debug)
        pipe.parameters['fetch']['last_sync_time'] = pipe.get_sync_time(debug=debug)
        _lst = pipe.get_sync_time(debug=debug)
        return naive_result

    sync_time = pipe.get_sync_time(debug=debug)
    if sync_time is None:
        naive_result = _naive_sync(pipe, with_extras=with_extras, debug=debug)
        pipe.parameters['fetch']['last_sync_time'] = pipe.get_sync_time(debug=debug)
        _lst = pipe.get_sync_time(debug=debug)
        return naive_result
        
    if sync_time.month != last_sync_time.month:
        result = monthly_sync_function(pipe, with_extras=with_extras, debug=debug)
    else:
        fetched_df = _simple_fetch(pipe, debug=debug)
        filtered_df = pipe.filter_existing(fetched_df, debug=debug)
        success_tuple = pipe.sync(filtered_df, check_existing=False, debug=debug)
        result = (success_tuple, filtered_df, fetched_df) if with_extras else success_tuple

    pipe.parameters['fetch']['last_sync_time'] = sync_time
    return result

def _rowcount_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ) -> Union[SuccessTuple, Tuple[SuccessTuple, 'pd.DataFrame']]:
    """
    Count the daily rowcounts and only sync months with different rowcounts.
    """
    from meerschaum.connectors.sql.tools import sql_item_name
    from meerschaum.utils.misc import filter_unseen_df
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas()

    new_dfs = []
    fetched_dfs = []

    fetched_df = _simple_fetch(pipe, debug=debug)
    filtered_df = pipe.filter_existing(fetched_df, debug=debug)
    new_dfs.append(filtered_df)
    fetched_dfs.append(fetched_df)
    success_tuple = pipe.sync(
        filtered_df,
        check_existing = False,
        debug = debug,
    )

    ### NOTE: This will need to be written for different flavors.
    ### See meerschaum.connectors.sql.tools.dateadd_str() for an example.
    ### For this experiment, PostgreSQL syntax will work just fine.
    source_query = f"""
    WITH definition AS ({pipe.parameters['fetch']['definition']})
    SELECT
      DATE_TRUNC('day', {sql_item_name(pipe.columns['datetime'], pipe.connector.flavor)}) AS days,
      COUNT(*) AS rowcount
    FROM definition
    GROUP BY days
    """
    target_query = f"""
    SELECT
      DATE_TRUNC('day', {sql_item_name(pipe.columns['datetime'], pipe.instance_connector.flavor)}) AS days,
      COUNT(*) AS rowcount
    FROM {sql_item_name(str(pipe), pipe.instance_connector.flavor)}
    GROUP BY days
    """

    source_rowcounts_df = pipe.connector.read(source_query, debug=debug)
    target_rowcounts_df = pipe.instance_connector.read(target_query, debug=debug)
    diff = filter_unseen_df(source_rowcounts_df, target_rowcounts_df, debug=debug)
    for index, row in diff.iterrows():
        begin = row['days'].to_pydatetime()
        end = begin + datetime.timedelta(days=1)
        fetched_df = pipe.fetch(begin=begin, end=end)
        filtered_df = pipe.filter_existing(fetched_df, debug=debug)
        new_dfs.append(filtered_df)
        fetched_dfs.append(fetched_df)
        success_tuple = pipe.sync(
            filtered_df,
            check_existing = False,
            debug = debug
        )

    if with_extras:
        return success_tuple, pd.concat(new_dfs), pd.concat(fetched_dfs)
    return success_tuple

def _unbounded_dynamic_iterative_cpi_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Iterative across the pipe's interval and perform CPISync on each interval.
    """
    return _generic_iterate_sync(
        pipe,
        fetch_function = _cpi_fetch,
        check_existing = False,
        with_extras = with_extras,
        debug = debug,
    )

def _unbounded_static_iterative_cpi_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Iterative across the pipe's interval and perform CPISync on each interval.
    """
    return _generic_iterate_sync(
        pipe,
        fetch_function = _cpi_fetch,
        check_existing = False,
        grow_bti = False,
        with_extras = with_extras,
        debug = debug,
    )


def _bounded_dynamic_iterative_cpi_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Iterative across the pipe's interval and perform CPISync on each interval.
    Stop iterating past 720 hours.
    """
    return _generic_iterate_sync(
        pipe,
        fetch_function = _cpi_fetch,
        check_existing = False,
        max_traversal_interval = datetime.timedelta(hours=720),
        with_extras = with_extras,
        debug = debug,
    )

def _bounded_static_iterative_cpi_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Iterative across the pipe's interval and perform CPISync on each interval.
    Stop iterating past 720 hours.
    Use a static BTI of 24 hours.
    """
    return _generic_iterate_sync(
        pipe,
        fetch_function = _cpi_fetch,
        check_existing = False,
        bti = datetime.timedelta(hours=24),
        grow_bti = False,
        max_traversal_interval = datetime.timedelta(hours=720),
        with_extras = with_extras,
        debug = debug,
    )


def _unbounded_dynamic_iterative_binary_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Iterative across the pipe's interval and perform a binary search sync on each interval.
    """
    return _generic_iterate_sync(
        pipe,
        fetch_function = _binary_fetch,
        check_existing = True,
        with_extras = with_extras,
        debug = debug,
    )

def _bounded_dynamic_iterative_binary_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Iterative across the pipe's interval and perform a binary search sync on each interval.
    """
    return _generic_iterate_sync(
        pipe,
        fetch_function = _binary_fetch,
        check_existing = True,
        with_extras = with_extras,
        max_traversal_interval = datetime.timedelta(hours=720),
        debug = debug,
    )

def _unbounded_static_iterative_binary_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Iterative across the pipe's interval and perform a binary search sync on each interval.
    """
    return _generic_iterate_sync(
        pipe,
        fetch_function = _binary_fetch,
        check_existing = True,
        bti = datetime.timedelta(hours=24),
        grow_bti = False,
        with_extras = with_extras,
        debug = debug,
    )

def _bounded_static_iterative_binary_sync(
        pipe: Pipe,
        with_extras: bool = False,
        debug: bool = False,
        **kw
    ):
    """
    Iterative across the pipe's interval and perform a binary search sync on each interval.
    """
    return _generic_iterate_sync(
        pipe,
        fetch_function = _binary_fetch,
        check_existing = True,
        with_extras = with_extras,
        bti = datetime.timedelta(hours=24),
        grow_bti = False,
        max_traversal_interval = datetime.timedelta(hours=720),
        debug = debug,
    )


fetch_methods = {
    'simple': _simple_fetch,
    'simple-backtrack': _simple_backtrack_fetch,
    'simple-slow-id': _simple_slow_id_fetch,
    'append': _append_fetch,
    'join': _join_fetch,
}
sync_methods = {
    'naive': _naive_sync,
    'unbounded-dynamic-iterative-simple': _unbounded_dynamic_iterative_simple_sync,
    'unbounded-static-iterative-simple': _unbounded_static_iterative_simple_sync,
    'bounded-dynamic-iterative-simple': _unbounded_dynamic_iterative_simple_sync,
    'bounded-static-iterative-simple': _unbounded_static_iterative_simple_sync,
    'simple-monthly-naive': _simple_monthly_flush_sync,
    'simple-monthly-cpi':  _simple_monthly_unbounded_dynamic_iterative_cpi_sync,
    'simple-monthly-binary':  _simple_monthly_unbounded_dynamic_iterative_binary_sync,
    'daily-rowcount': _rowcount_sync,
    'unbounded-dynamic-iterative-cpi': _unbounded_dynamic_iterative_cpi_sync,
    'unbounded-static-iterative-cpi': _unbounded_dynamic_iterative_cpi_sync,
    'bounded-dynamic-iterative-cpi': _bounded_dynamic_iterative_cpi_sync,
    'bounded-static-iterative-cpi': _bounded_static_iterative_cpi_sync,
    'unbounded-dynamic-iterative-binary': _unbounded_dynamic_iterative_binary_sync,
    'unbounded-static-iterative-binary': _unbounded_static_iterative_binary_sync,
    'bounded-static-iterative-binary': _bounded_static_iterative_binary_sync,
}
