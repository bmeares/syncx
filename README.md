# The `syncx` Plugin

This Meerschaum plugin implements experimental synchronization strategies which are discussed in my [master's thesis](https://meerschaum.io/files/pdf/thesis.pdf). Consult the [presentation slides](https://meerschaum.io/files/pdf/slides.pdf) for a summary of the strategies or read below for a brief overview.

You can [download the results tarball](https://meerschaum.io/files/syncx_results.tar.gz) or [browse the figures](https://meerschaum.io/files/syncx_results/figures/), which includes figures like the following:

![Daily performances](https://meerschaum.io/files/syncx_results/figures/winners_unknown-backlog_lines.png)

![Summary radar chart](https://meerschaum.io/files/syncx_results/figures/unbounded_unknown-backlog_radar.png)

![Choice Indices](https://meerschaum.io/files/syncx_results/figures/zummary_unknown-backlog_choice_2_priorities.png)

## Strategies Overview

The strategies are implemented in the `methods.py` module and are organized into three classes:

- **Simple Syncs**  
  Minimize run-time and bandwidth at the expense of accuracy.
  - *Simple Sync*  
  Select all rows newer than the latest target datetime value.
  - *Simple Backtrack Sync*  
  Select rows newer than a “walked back” latest target datetime value.
  - *Simple Slow-ID Sync*  
  Select rows newer than the oldest datetime of each ID’s latest datetime values.
  - *Simple Append Sync*  
  Generate *Simple Sync* queries for each ID and append them into a single transaction.
  - *Simple Join Sync*  
  Left join a temporary table of latest datetime values to emulate *Simple Sync* per each ID.
- **Iterative Syncs**  
  Guarantee perfect accuracy by considering the entire datetime axis.
  - *Iterative Simple Sync*  
  For each partition of the datetime axis, compare row-counts and perform Simple Sync when row-counts differ.
  - *Daily Row-Count Sync*  
  Build a table of days’ row-counts and perform Simple Sync on days with differing row-counts.
  - *Binary Search Sync*  
  For each partition of the datetime axis, compare row-counts and recursively binary search partitions with different row-counts until sufficiently small intervals are encountered. Perform *Simple Sync* on the small intervals.
  - *Iterative CPISync*  
  For each partition of the datetime axis, compare row-counts and perform *CPISync* when row-counts differ.
- **Corrective Syncs**  
  Perform *Simple Sync* daily and an iterative strategy monthly.

## Installation
```bash
mrsm install plugin syncx
```
Or clone into the plugins directory:
```bash
git clone https://github.com/bmeares/syncx ~/.config/meerschaum/plugins/syncx
```

## Usage
Run the included `start.sh` script followed by the number of the batch:
0. Baseline (*Naïve Sync* vs *Simple Sync*)
1. *Simple Syncs*
2. *Iterative Syncs*
3. *Corrective Syncs*

```bash
./start.sh 0 1 2 3
```

After running the simulations, generate the figures with the `results.py` script. Figures will be placed in `~/syncx_results/`.
```bash
python results.py
```

### Advanced Usage
Alternatively, use the `scenarios` action which generates run-time, bandwidth, and accuracy data in the `~/.config/meerschaum/plugins/syncx/scenarios/` folder.

```bash
mrsm scenarios unknown-backlog --sync-methods simple naive
```

The `scenarios` action supports the following flags:

```
scenario (positional arguments)
  The scenarios to simulate.
  Defaults to all scenarios.

  Options:
    - single-append-only
    - multiple-large-n-append-only
    - single-known-backlog
    - unknown-backlog

--begin
  The datetime to start the simulation.
  Defaults to '2021-01-01 00:00:00'

--end
  The datetime to end the simulation.
  Defaults to '2022-01-01 00:00:00'

--iterations
  How many times to run each simulation.
  Results will be the average of all iterations
    (to reduce noise in the run-time calculations).
  Defaults to 1.

--source
  SQLConnector of the source database
  Defaults to 'sql:memory'

--target
  SQLConnector of the target database
  Defaults to 'sql:memory'

--debug
  Verbosity toggle.
  Defaults to `False.`

--sync-methods
  Strategies to test.

  Options:
    - naive

    ### Simple Syncs
    - simple
    - simple-backtrack
    - simple-slow-id
    - append
    - join

    ### Iterative Syncs
    - unbounded-simple  /
        bounded-simple

    - unbounded-cpi  /
        bounded-cpi

    - unbounded-binary  /
        bounded-binary

    - unbounded-daily-rowcount  /
        bounded-daily-rowcount

    ### Corrective Syncs
    - simple-monthly-naive

    - simple-monthly-iterative-simple  /
      simple-monthly-bounded-simple

    - simple-monthly-cpi  /
      simple-monthly-bounded-cpi

    - simple-monthly-binary  /
      simple-monthly-bounded-binary

    - simple-monthly-daily-rowcount  /
      simple-monthly-bounded-daily-rowcount
```
