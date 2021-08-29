#! /bin/sh

ITERATIONS=10
RUN1=01_naive_simple
RUN2=02_simple_simple-backtrack_simple-slow-id_append_join
RUN3=03_simple_simple-monthly-flush_rowcount
RUN4=04_iterative_simples
RUN5=05_iterative_cpis
SYNCX_DIR=~/.config/meerschaum/plugins/syncx
BACKUP_DIR=~/syncx_results/
mkdir -p $BACKUP_DIR

python -m meerschaum scenarios --iterations $ITERATIONS --sync-methods naive simple
mkdir -p $BACKUP_DIR/$RUN1
cp -r $SYNCX_DIR/figures $BACKUP_DIR/$RUN1
cp -r $SYNCX_DIR/csv $BACKUP_DIR/$RUN1

python -m meerschaum scenarios --iterations $ITERATIONS --sync-methods simple simple-backtrack simple-slow-id append join
mkdir -p $BACKUP_DIR/$RUN2
cp -r $SYNCX_DIR/figures $BACKUP_DIR/$RUN2
cp -r $SYNCX_DIR/csv $BACKUP_DIR/$RUN2


python -m meerschaum scenarios --iterations $ITERATIONS --sync-methods simple simple-monthly-flush rowcount
mkdir -p $BACKUP_DIR/$RUN3
cp -r $SYNCX_DIR/figures $BACKUP_DIR/$RUN3
cp -r $SYNCX_DIR/csv $BACKUP_DIR/$RUN3

python -m meerschaum scenarios --iterations $ITERATIONS --sync-methods simple unbounded-dynamic-iterative-simple unbounded-static-iterative-simple bounded-dynamic-iterative-simple bounded-static-iterative-simple
mkdir -p $BACKUP_DIR/$RUN4
cp -r $SYNCX_DIR/figures $BACKUP_DIR/$RUN4
cp -r $SYNCX_DIR/csv $BACKUP_DIR/$RUN4

python -m meerschaum scenarios --iterations $ITERATIONS --sync-methods simple unbounded-dynamic-iterative-cpi unbounded-static-iterative-cpi bounded-dynamic-iterative-cpi bounded-static-iterative-cpi
mkdir -p $BACKUP_DIR/$RUN5
cp -r $SYNCX_DIR/figures $BACKUP_DIR/$RUN5
cp -r $SYNCX_DIR/csv $BACKUP_DIR/$RUN5

