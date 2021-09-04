#! /bin/bash

ITERATIONS=1
BEGIN=2021-01-01
END=2022-01-01
SYNCX_DIR=~/.config/meerschaum/plugins/syncx
SOURCE=sql:memory
TARGET=sql:memory
SCENARIOS="single-append-only multiple-large-n-append-only single-known-backlog unknown-backlog"
BACKUP_DIR=~/syncx_results/
mkdir -p $BACKUP_DIR

RUNS=("00_baseline" "01_simples" "02_iteratives" "03_correctives")
STRATS=(
  ### Baseline
  "simple naive"

  ### Simples
  "simple simple-backtrack simple-slow-id append join"

  ### Iteratives
  "simple daily-rowcount unbounded-simple unbounded-cpi unbounded-binary bounded-simple bounded-cpi bounded-binary"

  ### Correctives
  "simple simple-monthly-naive simple-monthly-cpi simple-monthly-binary simple-monthly-bounded-simple simple-monthly-bounded-cpi simple-monthly-bounded-binary"
)

for i in "${@}"; do
  echo Starting run \'${RUNS[$i]}\'...
  time python -m meerschaum scenarios $SCENARIOS\
    --sync-methods ${STRATS[$i]} \
    --iterations $ITERATIONS --begin $BEGIN --end $END --source $SOURCE --target $TARGET
  mkdir -p $BACKUP_DIR/${RUNS[$i]}
  cp -r $SYNCX_DIR/scenarios $BACKUP_DIR/${RUNS[$i]}
done

