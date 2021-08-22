#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Simulate the various scenarios described in Chapter 2 of the thesis.
"""

from __future__ import annotations
from dataclasses import dataclass
from meerschaum.utils.typing import Dict, SuccessTuple
from meerschaum.connectors.sql import SQLConnector
from meerschaum.utils.debug import dprint
from meerschaum.utils.warnings import info
import datetime, random

SIMULATION_INTERVAL = datetime.timedelta(days=365)
SIMULATION_STEPSIZE = datetime.timedelta(days=1)


@dataclass
class Scenario:
    """
    Contain the metadata for setting up a scenario.
    """

    source_connector: SQLConnector
    target_connector: SQLConnector

    name: str

    ### The interval between rows in seconds, e.g. 60 -> 1 row per minute
    frequency_seconds: int = 60

    ### How many sub-streams within the pipe.
    num_ids: int = 1

    ### How many rows the table starts with before synchronization.
    initial_rowcount = 1000

    ### Toggle mutability.
    immutable: bool = True

    ### The maximum number of seconds rows may be backlogged into the table.
    ### 0 means no backlogging, and `None` means unbounded (any datetime).
    max_backlog_seconds: int = 0

    ### If 'sql', use complex SQL queries.
    ### Else use simple bounded queries.
    protocol: str = 'sql'

    @property 
    def pipe(self):
        from meerschaum import Pipe
        from meerschaum.connectors.sql.tools import sql_item_name
        if '_pipe' not in self.__dict__:
            self._pipe = Pipe(
                'plugin:syncx', self.name, parameters = {
                    'columns': {
                        'datetime': 'datetime',
                        'id': 'id',
                    },
                    'fetch': {
                        'definition': (
                            "SELECT * FROM "
                            + sql_item_name(self.name, self.source_connector.flavor)
                        ),
                    },
                }
            )

        return self._pipe

    def init_source_table(
        self,
        now: datetime.datetime,
        debug: bool = False,
    ) -> None:
        """
        Create the source table with `initial_rowcount` rows (divided per ID).
        """
        from meerschaum.utils.packages import import_pandas
        from meerschaum.utils.misc import round_time
        pd = import_pandas()
        data = {
            'datetime': [],
            'id': [],
            'value': [],
        }
        dtypes = {
            'datetime': 'datetime64[ns]',
            'id': int,
            'value': float,
        }

        next_id = 1
        for _id_rownum in range(self.initial_rowcount):
            now = now - datetime.timedelta(seconds=self.frequency_seconds)

            data['datetime'].append(now)
            data['id'].append(next_id)
            data['value'].append(get_value())

            next_id = max((next_id + 1) % (self.num_ids + 1), 1)

        for col in data:
            data[col] = pd.Series(data[col], dtype=dtypes[col])

        df = pd.DataFrame(data)
        self.source_connector.to_sql(df, name=self.name, if_exists='replace', debug=debug)

    def init_target_table(
        self,
        now: datetime.datetime,
        debug: bool = False,
        **kw
    ) -> None:
        """
        Initialize the target tables.
        """
        from meerschaum.utils.misc import round_time
        from meerschaum.connectors.sql.tools import sql_item_name
        data = {
            'datetime': [now],
            'id': [1],
            'value': [1.0],
        }
        self.pipe.drop(debug=debug)
        self.pipe.sync(data, debug=debug)
        self.target_connector.exec(
            f"DELETE FROM {sql_item_name(str(self.pipe), self.target_connector.flavor)} "
            + "WHERE 1 = 1",
            debug=debug,
        )


    def sync_target_table(
        self,
        **kw
    ) -> SuccessTuple:
        """
        Execute `pipe.sync()` for the target table.
        """
        return self.pipe.sync(deactivate_plugin_venv=False, **kw)

    def advance_source_table(
        self,
        now: datetime.datetime,
        time_step_interval: datetime.timedelta,
        debug: bool=False,
    ) -> None:
        """
        Advance the simulation of the source table by `time_step_interval`.
        Depending on the properties of the scenario, some rows may be backlogged.
        """
        from meerschaum.utils.packages import import_pandas
        pd = import_pandas()
        elapsed_seconds = time_step_interval.total_seconds()
        data = {
            'datetime': [],
            'id': [],
            'value': [],
        }

        for i in range(int(elapsed_seconds / self.frequency_seconds)):
            for _id in range(1, self.num_ids + 1):
                data['datetime'].append(now)
                data['id'].append(_id)
                data['value'].append(get_value())
            now = now + datetime.timedelta(seconds=self.frequency_seconds)

        df = pd.DataFrame(data)
        self.source_connector.to_sql(df, name=self.name, if_exists='append', debug=debug)


    def start(
        self,
        fetch_method: str,
        debug: bool = False,
    ) -> SuccessTuple:
        """
        Run the simulation for this scenario.
        """
        import time
        from meerschaum import Pipe
        from meerschaum.utils.misc import round_time

        #  now = round_time(datetime.datetime.utcnow(), datetime.timedelta(minutes=15))
        now = datetime.datetime(2021, 1, 1, 0, 0)

        ### Create the source table and target pipe.
        info(f"Initializing {self.initial_rowcount} rows for scenario '{self.name}'...")
        self.init_source_table(now, debug=debug)
        self.init_target_table(now, debug=debug)

        end_time = now + SIMULATION_INTERVAL

        runtimes_data = {'Datetime': [], 'Runtime': []}
        monthly_runtimes_data = {'Month': [], 'Runtime': []}

        last_month = None
        while now < end_time:
            _lm = get_last_month(now)
            if _lm != last_month:
                last_month = _lm
                print("Simulating " + now.strftime('%B %Y') + f" for scenario '{self.name}' with fetch method '{fetch_method}'...")

            self.advance_source_table(now, SIMULATION_STEPSIZE, debug=debug)
            
            _start_sync_runtime = time.time()
            self.sync_target_table(
                fetch_method = fetch_method,
                ### Skip BTI if `append-only`.
                check_existing = ('append-only' not in self.name),
                debug = debug
            )
            runtimes_data['Datetime'].append(now)
            runtimes_data['Runtime'].append(time.time() - _start_sync_runtime)

            now = now + SIMULATION_STEPSIZE

        return runtimes_data



def init_scenarios(
        source_connector,
        target_connector,
    ) -> Dict[str, Scenario]:
    """
    Build the scenarios dictionary.
    """
    scenarios_list = [
        Scenario(source_connector, target_connector, name='single-append-only'),
        Scenario(
            source_connector, target_connector,
            name = 'multiple-append-only',
            num_ids = 3,
        ),
        Scenario(
            source_connector, target_connector,
            name = 'single-known-backlog',
            max_backlog_seconds = 3600,
        ),
        Scenario(
            source_connector, target_connector,
            name = 'multiple-known-backlog',
            num_ids = 3,
            max_backlog_seconds = 3600,
        ),
        Scenario(
            source_connector, target_connector,
            name = 'unknown-backlog-simple',
            num_ids = 3,
            max_backlog_seconds = None,
            protocol = 'samples',
        ),
        Scenario(
            source_connector, target_connector,
            name = 'unknown-backlog-sql',
            num_ids = 3,
            max_backlog_seconds = None,
            protocol = 'sql',
        ),
        Scenario(
            source_connector, target_connector,
            name = 'mutable-samples',
            num_ids = 3,
            immutable = False,
            max_backlog_seconds = None,
            protocol = 'samples',
        ),
        Scenario(
            source_connector, target_connector,
            name = 'mutable-hashing',
            num_ids = 3,
            immutable = False,
            max_backlog_seconds = None,
            protocol = 'sql',
        ),
    ]
    return {scenario.name: scenario for scenario in scenarios_list}

def get_value():
    return random.random() * 100

def get_last_month(dt: datetime.datetime) -> int:
    """
    Return the integer value of the previous month.
    """
    return (dt.replace(day=1) - datetime.timedelta(days=1)).month
