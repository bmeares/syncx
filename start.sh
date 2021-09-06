#! /bin/bash

ITERATIONS=1
BEGIN=2021-01-01
END=2022-01-01
SYNCX_DIR=~/.config/meerschaum/plugins/syncx
SOURCE=sql:memory
TARGET=sql:memory
SCENARIOS="single-append-only multiple-large-n-append-only single-known-backlog unknown-backlog"
COOLDOWN=10
BACKUP_DIR=~/syncx_results/
mkdir -p $BACKUP_DIR

DATASETS=("cumulative_volume" "daily_runtime" "daily_volume" "error_rate" "errors" "monthly_runtime")
RUNS=("00_baseline" "01_simples" "02_iteratives" "03_correctives")
STRATS=(
  ### Baseline
  "simple simple-backtrack"

  ### Simples
  "simple-backtrack simple-slow-id append join"

  ### Iteratives
  "daily-rowcount unbounded-simple unbounded-cpi unbounded-binary bounded-simple bounded-cpi bounded-binary"

  ### Correctives
  "simple-monthly-naive simple-monthly-cpi simple-monthly-binary simple-monthly-bounded-simple simple-monthly-bounded-cpi simple-monthly-bounded-binary"
)

for i in "${@}"; do
  echo Starting run \'${RUNS[$i]}\'...
  mkdir -p $BACKUP_DIR/${RUNS[$i]}
  for strategy in ${STRATS[$i]}; do
    for scenario in $SCENARIOS; do

      ### Run the simulation for the specific scenario/strategy combo.
      time python -m meerschaum scenarios $scenario \
        --sync-methods $strategy \
        --iterations $ITERATIONS --begin $BEGIN --end $END --source $SOURCE --target $TARGET

      ### Append the new columns from the CSVs we just generated to the backup.
      for dataset in "${DATASETS[@]}"; do
        old_csv_dir_path=$BACKUP_DIR/${RUNS[$i]}/scenarios/$scenario/csv
        old_csv_path="$old_csv_dir_path"/"$scenario"_"$dataset".csv
        new_csv_dir_path=$SYNCX_DIR/scenarios/$scenario/csv
        new_csv_path="$new_csv_dir_path"/"$scenario"_"$dataset".csv

        if [ ! -f "$new_csv_path" ]; then
          echo "Missing file '$new_csv_path'! Was the simulation killed?"
        elif [ ! -f "$old_csv_path" ]; then
          mkdir -p "$old_csv_dir_path"
          cp $new_csv_path $old_csv_dir_path
        else
          python $SYNCX_DIR/add_to_csv.py $old_csv_path $new_csv_path
        fi

        echo "Cooldown! Sleeping for $COOLDOWN seconds."
        sleep $COOLDOWN

      done
    done
  done
done

