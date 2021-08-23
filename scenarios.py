#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Simulate the various scenarios described in Chapter 2 of the thesis.
"""

from __future__ import annotations
from dataclasses import dataclass
from meerschaum.utils.typing import Dict, SuccessTuple, List, Optional
from meerschaum.connectors.sql import SQLConnector
from meerschaum.utils.debug import dprint
from meerschaum.utils.warnings import info
import datetime, random
from collections import namedtuple

Row = namedtuple('Row', ('datetime', 'id', 'value'))
Row_dtypes = {
    'datetime': 'datetime64[ns]',
    'id': int,
    'value': float,
}

SIMULATION_INTERVAL = datetime.timedelta(days=365)
SIMULATION_STEPSIZE = datetime.timedelta(days=1)
BACKLOG_PROBABILITY_MIN = 1
BACKLOG_PROBABILITY_MAX = 100
BACKLOG_PROBABILITY_THRESHOLD = 2
ITERATIONS_PER_SCENARIO_FM = 10


@dataclass
class Scenario:
    """
    Contain the metadata for setting up a scenario.
    """

    source_connector: SQLConnector
    target_connector: SQLConnector

    name: str

    ### The interval between rows in seconds, e.g. 60 -> 1 row per minute
    frequency_seconds: int = 60 * 15

    ### How many sub-streams within the pipe.
    num_ids: int = 1

    ### How many rows the table starts with before synchronization.
    initial_rowcount = 0

    ### Toggle mutability.
    immutable: bool = True

    ### The maximum number of seconds rows may be backlogged into the table.
    ### 0 means no backlogging, and `None` means unbounded (any datetime).
    max_backlog_seconds: int = 0

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

    @property
    def outages(self) -> List[Row]:
        """
        List of generated records skipped due to an 'outage'.
        """
        if '_outages' not in self.__dict__:
            self._outages = []
        return self._outages

    @outages.setter
    def outages(self, _outages) -> None:
        self._outages = _outages

    def is_outage(self):
        if self.max_backlog_seconds == 0:
            return False
        return random.randint(
            BACKLOG_PROBABILITY_MIN,
            BACKLOG_PROBABILITY_MAX
        ) <= BACKLOG_PROBABILITY_THRESHOLD

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

        next_id = 1
        for _id_rownum in range(self.initial_rowcount):
            now = now - datetime.timedelta(seconds=self.frequency_seconds)

            data['datetime'].append(now)
            data['id'].append(next_id)
            data['value'].append(get_value())

            next_id = max((next_id + 1) % (self.num_ids + 1), 1)

        for col in data:
            data[col] = pd.Series(data[col], dtype=Row_dtypes[col])

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

        data: List[Row] = []

        for i in range(int(elapsed_seconds / self.frequency_seconds)):
            for _id in range(1, self.num_ids + 1):
                row = Row(now, _id, get_value())
                if self.is_outage():
                    self.outages.append(row)
                    continue
                data.append(row)
            now = now + datetime.timedelta(seconds=self.frequency_seconds)

        df = pd.DataFrame(data)
        self.source_connector.to_sql(df, name=self.name, if_exists='append', debug=debug)

    def backlog_source_table(
        self,
        now: datetime.datetime,
        time_step_interval: datetime.timedelta,
        debug: bool = False,
    ) -> None:
        """
        Insert backlogged records into the source table.
        """
        from meerschaum.utils.packages import import_pandas
        pd = import_pandas()

        records_ceiling = None
        expired_i = None
        st, et = now, now + time_step_interval
        for i, row in enumerate(self.outages):
            elapsed_seconds = (row.datetime - st).total_seconds()
            if elapsed_seconds > 0:
                records_ceiling = i
                break

            next_elapsed_seconds = (et - row.datetime).total_seconds()
            if (
                self.max_backlog_seconds is not None
                and next_elapsed_seconds > self.max_backlog_seconds
            ):
                expired_i = i

        ### No records are old enough to be backlogged.
        if (
            not self.outages
            or records_ceiling == 0
            or (
                self.max_backlog_seconds is not None
                and expired_i is None
            )
        ):
            return

        ### If backlogging is unbounded, randomly choose a subset of the old records.
        ### Otherwise, only choose records that will 'expire' by the next iteration.
        rows_to_add_index = (
            random.randint(0, records_ceiling - 1) if self.max_backlog_seconds is None
            else expired_i
        )
        df = pd.DataFrame(self.outages[:rows_to_add_index + 1])
        self.outages = self.outages[rows_to_add_index + 1:]
        self.source_connector.to_sql(df, name=self.name, if_exists='append', debug=debug)


    def calcuate_error(self, debug: bool = False) -> int:
        """
        After synchronization, count the number of missing rows between
        the source and target tables.
        """
        from meerschaum.utils.misc import filter_unseen_df
        chunksize = 10000
        source_df = self.source_connector.read(self.name, chunksize=chunksize, debug=debug)
        target_df = self.pipe.get_data(chunksize=chunksize, debug=debug)
        return len(filter_unseen_df(source_df, target_df, debug=debug))


    def start(
        self,
        fetch_method: str,
        debug: bool = False,
    ) -> Tuple[Dict[str, List[Union[datetime.datetime, float]]], int]:
        """
        Run the simulation for this scenario.
        Return a tuple of runtimes data dictionary and number of missed rows.
        """
        import time
        from meerschaum import Pipe
        from meerschaum.utils.misc import round_time

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
                print(
                    "Simulating " + now.strftime('%B %Y')
                    + f" for scenario '{self.name}' with fetch method '{fetch_method}'..."
                )

            self.advance_source_table(now, SIMULATION_STEPSIZE, debug=debug)
            self.backlog_source_table(now, SIMULATION_STEPSIZE, debug=debug)
            
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

        print(f"Calculating errors between source and target tables (this might take awhile)...")
        error = self.calcuate_error(debug=debug)

        return runtimes_data, error



def init_scenarios(
        source_connector,
        target_connector,
    ) -> Dict[str, Scenario]:
    """
    Build the scenarios dictionary.
    """
    small_n = 3
    large_n = 99
    scenarios_list = [
        Scenario(
            source_connector, target_connector,
            name = 'single-append-only',
            num_ids = 1,
            max_backlog_seconds = 0,
            immutable = True,
        ),
        Scenario(
            source_connector, target_connector,
            name = 'multiple-small-n-append-only',
            num_ids = small_n,
            max_backlog_seconds = 0,
            immutable = True,
        ),
        Scenario(
            source_connector, target_connector,
            name = 'multiple-large-n-append-only',
            num_ids = large_n,
            max_backlog_seconds = 0,
            immutable = True,
        ),
        Scenario(
            source_connector, target_connector,
            name = 'single-known-backlog',
            num_ids = 1,
            ### BTI = 24 hours 
            max_backlog_seconds = 86400,
            immutable = True,
        ),
        Scenario(
            source_connector, target_connector,
            name = 'multiple-small-n-known-backlog',
            num_ids = small_n,
            max_backlog_seconds = 86400,
            immutable = True,
        ),
        Scenario(
            source_connector, target_connector,
            name = 'multiple-large-n-known-backlog',
            num_ids = large_n,
            max_backlog_seconds = 86400,
            immutable = True,
        ),
        Scenario(
            source_connector, target_connector,
            name = 'unknown-backlog-simple',
            num_ids = small_n,
            max_backlog_seconds = None,
            immutable = True,
        ),
        #  Scenario(
            #  source_connector, target_connector,
            #  name = 'unknown-backlog-sql',
            #  num_ids = small_n,
            #  max_backlog_seconds = None,
            #  immutable = True,
        #  ),
        #  Scenario(
            #  source_connector, target_connector,
            #  name = 'mutable-samples',
            #  num_ids = small_n,
            #  immutable = False,
            #  max_backlog_seconds = None,
        #  ),
        #  Scenario(
            #  source_connector, target_connector,
            #  name = 'mutable-hashing',
            #  num_ids = small_n,
            #  immutable = False,
            #  max_backlog_seconds = None,
        #  ),
    ]
    return {scenario.name: scenario for scenario in scenarios_list}

def get_value():
    return random.random() * 100

def get_last_month(dt: datetime.datetime) -> int:
    """
    Return the integer value of the previous month.
    """
    return (dt.replace(day=1) - datetime.timedelta(days=1)).month
