    # #initialize connection
    # conn = sqlite3.connect('inflation_database.db')
    # cursor = conn.cursor()
    
    # #execute create table query 
    # create_table_from_dataframe(cursor, index_df, 'inflation_index')

    # #insert data into query
    # index_df.to_sql('inflation_index', conn, if_exists='append', index=False)