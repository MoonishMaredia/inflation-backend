"""Initialize a new local database with the historical CSV snapshot."""
from contextlib import closing
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory

import pandas as pd

from config import DB_PATH, ROOT

if __name__ == "__main__":
    if DB_PATH.exists():
        raise SystemExit(f"Refusing to replace existing database: {DB_PATH}")

    # import files
    index_df = pd.read_csv(ROOT / 'datafiles/index_table_final.csv', index_col=False, usecols=['year', 'month', 'series_id', 'series_desc', 'level', 'order', 'value'])
    index_df.rename(columns={"order":"priority"}, inplace=True)
    index_df['date'] = pd.to_datetime(index_df[['year', 'month']].assign(day=1)).dt.strftime('%Y-%m-%d')

    weights_df = pd.read_csv(ROOT / 'datafiles/weights_table_final.csv', index_col=False, usecols=['year', 'month', 'series_id', 'series_desc', 'level', 'order', 'curr_weight'])
    weights_df.rename(columns={"order":"priority", "curr_weight":"weight"}, inplace=True)
    weights_df['date'] = pd.to_datetime(weights_df[['year', 'month']].assign(day=1)).dt.strftime('%Y-%m-%d')

    series_relation_df = pd.read_csv(ROOT / 'datafiles/series_relationship.csv', index_col=False, usecols=['series_id','series_desc', 'parent_category_series_id','parent_category_series_desc',
                                                                                                      'level', 'priority'])

    annual_base_weights_df = pd.read_csv(ROOT / 'datafiles/annual_weights.csv', index_col=False, usecols=['series_desc','series_id', 'base_weight', 'year'])
    with TemporaryDirectory(dir=DB_PATH.parent) as scratch:
        staged = Path(scratch) / DB_PATH.name
        with closing(sqlite3.connect(staged)) as conn:
            for table, frame in (
                ("inflation_index", index_df), ("weights_table", weights_df),
                ("series_relation", series_relation_df), ("base_weight", annual_base_weights_df),
            ):
                frame.to_sql(table, conn, if_exists="fail", index=False)
        DB_PATH.hardlink_to(staged)
    print(f"Created {DB_PATH}")
