#! /bin/sh

ITERATIONS=1
BEGIN=2021-01-01
END=2022-01-01
RUN1=01_baseline
RUN2=02_modified_simple
RUN3=03_corrective
RUN4=04_perfect
RUN5=05_almost_perfect

### Baseline
RUN1_STRATS="simple naive"

### Modified simple
RUN2_STRATS="simple simple-backtrack simple-slow-id append join"

### Corrective
RUN3_STRATS="simple simple-monthly-naive simple-monthly-cpi simple-monthly-binary"

### Guaranteed accuracy
RUN4_STRATS="simple daily-rowcount unbounded-dynamic-iterative-simple unbounded-dynamic-iterative-cpi unbounded-dynamic-iterative-binary"

### Mostly perfect accuracy
RUN5_STRATS="simple bounded-dynamic-iterative-simple bounded-dynamic-iterative-cpi bounded-dynamic-iterative-binary"

SYNCX_DIR=~/.config/meerschaum/plugins/syncx
SOURCE=sql:memory
TARGET=sql:memory
SCENARIOS="single-append-only multiple-large-n-append-only single-known-backlog unknown-backlog"
BACKUP_DIR=~/syncx_results/
mkdir -p $BACKUP_DIR


python -m meerschaum scenarios $SCENARIOS\
  --sync-methods $RUN1_STRATS \
  --iterations $ITERATIONS --begin $BEGIN --end $END --source $SOURCE --target $TARGET
mkdir -p $BACKUP_DIR/$RUN1
cp -r $SYNCX_DIR/scenarios $BACKUP_DIR/$RUN1


python -m meerschaum scenarios $SCENARIOS\
  --sync-methods $RUN2_STRATS \
  --iterations $ITERATIONS --begin $BEGIN --end $END --source $SOURCE --target $TARGET
mkdir -p $BACKUP_DIR/$RUN2
cp -r $SYNCX_DIR/scenarios $BACKUP_DIR/$RUN2


python -m meerschaum scenarios $SCENARIOS\
  --sync-methods $RUN3_STRATS \
  --iterations $ITERATIONS --begin $BEGIN --end $END --source $SOURCE --target $TARGET
mkdir -p $BACKUP_DIR/$RUN3
cp -r $SYNCX_DIR/scenarios $BACKUP_DIR/$RUN3


python -m meerschaum scenarios $SCENARIOS\
  --sync-methods $RUN4_STRATS \
  --iterations $ITERATIONS --begin $BEGIN --end $END --source $SOURCE --target $TARGET
mkdir -p $BACKUP_DIR/$RUN4
cp -r $SYNCX_DIR/scenarios $BACKUP_DIR/$RUN4


python -m meerschaum scenarios $SCENARIOS\
  --sync-methods $RUN5_STRATS \
  --iterations $ITERATIONS --begin $BEGIN --end $END --source $SOURCE --target $TARGET
mkdir -p $BACKUP_DIR/$RUN5
cp -r $SYNCX_DIR/scenarios $BACKUP_DIR/$RUN5

