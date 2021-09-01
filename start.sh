#! /bin/sh

ITERATIONS=1
BEGIN=2021-01-01
END=2021-03-01
RUN1=01_naive_simple
RUN2=02_simple_simple-backtrack_simple-slow-id_append_join
RUN3=03_simple_simple-monthly-flush_rowcount
RUN4=04_iterative_simples
RUN5=05_iterative_cpis
SYNCX_DIR=~/.config/meerschaum/plugins/syncx
SOURCE=sql:memory
TARGET=sql:memory
# SCENARIOS="single-append-only multiple-small-n-append-only multiple-large-n-append-only single-known-backlog multiple-small-n-known-backlog multiple-large-n-known-backlog unknown-backlog"
SCENARIOS="single-append-only multiple-large-n-append-only single-known-backlog unknown-backlog"
BACKUP_DIR=~/syncx_results/
mkdir -p $BACKUP_DIR

### BASLINE

python -m meerschaum scenarios $SCENARIOS\
  --sync-methods naive simple \
  --iterations $ITERATIONS --begin $BEGIN --end $END --source $SOURCE --target $TARGET
mkdir -p $BACKUP_DIR/$RUN1
cp -r $SYNCX_DIR/scenarios $BACKUP_DIR/$RUN1


### SIMPLE VARIATIONS

python -m meerschaum scenarios $SCENARIOS\
  --sync-methods simple simple-backtrack simple-slow-id append join \
  --iterations $ITERATIONS --begin $BEGIN --end $END --source $SOURCE --target $TARGET
mkdir -p $BACKUP_DIR/$RUN2
cp -r $SYNCX_DIR/scenarios $BACKUP_DIR/$RUN2


### SIMPLE CORRECT LATER

python -m meerschaum scenarios $SCENARIOS\
  --sync-methods simple simple-monthly-naive daily-rowcount simple-monthly-cpi \
  --iterations $ITERATIONS --begin $BEGIN --end $END --source $SOURCE --target $TARGET
mkdir -p $BACKUP_DIR/$RUN3
cp -r $SYNCX_DIR/scenarios $BACKUP_DIR/$RUN3


### ITERATIVE SIMPLE

python -m meerschaum scenarios $SCENARIOS\
  --sync-methods simple unbounded-dynamic-iterative-simple unbounded-static-iterative-simple bounded-dynamic-iterative-simple bounded-static-iterative-simple \
  --iterations $ITERATIONS --begin $BEGIN --end $END --source $SOURCE --target $TARGET
mkdir -p $BACKUP_DIR/$RUN4
cp -r $SYNCX_DIR/scenarios $BACKUP_DIR/$RUN4


### ITERATIVE CPI

python -m meerschaum scenarios $SCENARIOS\
  --sync-methods simple unbounded-dynamic-iterative-cpi unbounded-static-iterative-cpi bounded-dynamic-iterative-cpi bounded-static-iterative-cpi \
  --iterations $ITERATIONS --begin $BEGIN --end $END --source $SOURCE --target $TARGET
mkdir -p $BACKUP_DIR/$RUN5
cp -r $SYNCX_DIR/scenarios $BACKUP_DIR/$RUN5

