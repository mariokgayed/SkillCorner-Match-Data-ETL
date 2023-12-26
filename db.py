from sqlalchemy import create_engine, Table, MetaData

def WriteToDB(table_name):
    database_url = 'postgresql://username:password@localhost:5432/your_database'
    engine = create_engine(database_url)

    # Create a SQLAlchemy Table object
    metadata = MetaData()
    current_table = Table(table_name, metadata, autoload_with=engine)

    # Create or replace the table
    metadata.create_all(engine)

    # Insert or update the DataFrame into the table
    df.to_sql(table_name, engine, index=False, if_exists='append', method='multi', chunksize=500, index_label='id', dtype={'id': sqlalchemy.Integer})
