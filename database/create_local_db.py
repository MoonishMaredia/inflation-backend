import pandas as pd
import sqlite3

def create_table_from_dataframe(cursor, df, table_name):
    # columns = df.columns
    dtypes = df.dtypes
    sql_dtypes = []

    for column, dtype in dtypes.items():
        if dtype == 'int64':
            sql_dtypes.append(f'"{column}" INTEGER')
        elif dtype == 'float64':
            sql_dtypes.append(f'"{column}" REAL')
        elif dtype == 'object':
            sql_dtypes.append(f'"{column}" TEXT')
        elif dtype.name == 'datetime64[ns]':
            sql_dtypes.append(f'"{column}" TEXT')  # SQLite does not have a dedicated date type, so store as TEXT
        else:
            raise ValueError(f"Unhandled dtype: {dtype} for column {column}")

    col_defs = ", ".join(sql_dtypes)
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs});"    
    cursor.execute(create_table_query)

if __name__=="__main__":

    # import files
    index_df = pd.read_csv('./datafiles/index_table_final.csv', index_col=False, usecols=['year', 'month', 'series_id', 'series_desc', 'level', 'order', 'value'])
    index_df.rename(columns={"order":"priority"}, inplace=True)
    index_df['date'] = pd.to_datetime(index_df[['year', 'month']].assign(day=1)).dt.strftime('%Y-%m-%d')

    weights_df = pd.read_csv('./datafiles/weights_table_final.csv', index_col=False, usecols=['year', 'month', 'series_id', 'series_desc', 'level', 'order', 'curr_weight'])
    weights_df.rename(columns={"order":"priority", "curr_weight":"weight"}, inplace=True)
    weights_df['date'] = pd.to_datetime(weights_df[['year', 'month']].assign(day=1)).dt.strftime('%Y-%m-%d')

    series_relation_df = pd.read_csv('./datafiles/series_relationship.csv', index_col=False, usecols=['series_id','series_desc', 'parent_category_series_id','parent_category_series_desc', 
                                                                                                      'level', 'priority'])
    
    annual_base_weights_df = pd.read_csv('./datafiles/annual_weights.csv', index_col=False, usecols=['series_desc','series_id', 'base_weight', 'year'])
    #initialize connection
    conn = sqlite3.connect('inflation_database.db')
    cursor = conn.cursor()
    
    #execute create table query 
    #insert data into query
    create_table_from_dataframe(cursor, index_df, 'inflation_index')
    index_df.to_sql('inflation_index', conn, if_exists='replace', index=False)

    #execute create table query 
    #insert data into query
    create_table_from_dataframe(cursor, weights_df, 'weights_table')
    weights_df.to_sql('weights_table', conn, if_exists='replace', index=False)

    #execute create table query 
    #insert data into query
    create_table_from_dataframe(cursor, series_relation_df, 'series_relation')
    series_relation_df.to_sql('series_relation', conn, if_exists='replace', index=False)

    #execute create table query 
    #insert data into query
    create_table_from_dataframe(cursor, annual_base_weights_df, 'base_weight')
    annual_base_weights_df.to_sql('base_weight', conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()

    print("Created inflation_database.db")

