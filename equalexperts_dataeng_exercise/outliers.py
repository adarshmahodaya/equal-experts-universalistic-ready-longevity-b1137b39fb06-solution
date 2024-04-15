import json
import sys
import duckdb

def create_outlier_view(conn,schema_name,table_name,view_name):
    try:
        query = f"""
            create or replace view {schema_name}.{view_name} as
                select year_number,week_number,week_vote_count
                from (  select  EXTRACT(year FROM CreationDate) AS year_number,
                            EXTRACT(week FROM CreationDate) AS week_number,
                            count(*) as week_vote_count,
                            count(*) over () as total_weeks,
                            sum(week_vote_count) over () as total_votes,
                            total_votes/total_weeks as avg_votes_per_week,
                            abs(1-week_vote_count/avg_votes_per_week) as deviation
                        from {schema_name}.{table_name}
                        group by year_number,
                                week_number)
                where deviation > 0.2
        """
        print(conn.execute(query).fetchall)
    except Exception as e:
        print("Data load failed, error: ")
        return e
    
    result = 'view creation successful'
    return result

if __name__ == "__main__":

    databasefile = "warehouse.db"
    table_name = 'votes'
    schema_name = 'blog_analysis'
    view_name = 'outlier_weeks'

    conn = duckdb.connect(databasefile)

    view_creation_result = create_outlier_view(conn,schema_name,table_name,view_name)
    print(view_creation_result)