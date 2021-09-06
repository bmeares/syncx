#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Add a column to a CSV file (called from start.sh).
"""

import sys

def main():
    import pathlib
    usage = "Usage: python add_to_csv.py [old_file] [new_file]"
    if len(sys.argv) != 3:
        print(usage, file=sys.stderr)
        return 1

    existing_csv_path = pathlib.Path(sys.argv[1])
    new_csv_path = pathlib.Path(sys.argv[2])
    if not existing_csv_path.exists() or not new_csv_path.exists():
        print(usage, file=sys.stderr)
        return 1

    
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas()
    existing_df = pd.read_csv(existing_csv_path, index_col=0)
    new_df = pd.read_csv(new_csv_path, index_col=0)

    for col in new_df:
        if col not in existing_df.columns:
            existing_df[col] = new_df[col]

    existing_df.to_csv(existing_csv_path)

    return 0


if __name__ == '__main__':
    sys.exit(main())
