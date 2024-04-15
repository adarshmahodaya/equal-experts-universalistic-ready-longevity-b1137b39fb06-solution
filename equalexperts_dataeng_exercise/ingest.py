import json
import sys
import duckdb



def create_table_and_Schema(conn,schema_name,table_name,schema):
    
    try:
        
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}") # create schema

        conn.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({schema})") # create table

        conn.execute(f"drop table if exists {schema_name}.temp_table") # drop temp_table if exists
 

    except Exception as e:
        print('Schema and table creation failed, error:  ')
        return(e)
    
    result = 'Schema and table creation successful'
    return result

def write_json_to_db(conn,input_json_path,schema_name,table_name):
    
    #query to insert data into the table
    insert_query = f"""
        insert into {schema_name}.{table_name} 
        SELECT * 
        FROM read_json('{input_json_path}', format = 'newline_delimited',columns = {{'Id': 'VARCHAR',
                         'PostId': 'VARCHAR',
                         'VoteTypeId': 'VARCHAR',
                         'CreationDate': 'VARCHAR',
                         'UserId': 'VARCHAR',
                         'BountyAmount': 'VARCHAR'}});
    """
    #set of queries to handle duplicates if any
    remove_duplicate_query = f"""
        CREATE TABLE {schema_name}.temp_table AS
        SELECT DISTINCT * FROM {schema_name}.{table_name};

        DROP TABLE {schema_name}.{table_name};

        ALTER TABLE {schema_name}.temp_table RENAME TO {table_name};
    """
    
    try:
        conn.execute(insert_query)
    except Exception as e:
        print('insertion failed')
        return e
    
    conn.execute(remove_duplicate_query)
    
    return('data load complete')    


if __name__ == "__main__":

    input_json_path = sys.argv[1]
    databasefile = "warehouse.db"
    table_name = 'votes'
    schema_name = 'blog_analysis'
    schema = 'Id VARCHAR NOT NULL, PostId VARCHAR NOT NULL, VoteTypeId VARCHAR NOT NULL, CreationDate DATETIME NOT NULL, UserId VARCHAR, BountyAmount VARCHAR'
    
    try:
        
        conn = duckdb.connect(databasefile)
    
    except FileNotFoundError:
        print("Please download the dataset using 'poetry run exercise fetch-data'")
    

    table_and_schema_creation_result = create_table_and_Schema(conn,schema_name,table_name,schema)
    print(table_and_schema_creation_result)

    

    table_load_result = write_json_to_db(conn,input_json_path,schema_name,table_name)
    print(table_load_result)

    