import logging
import os
import subprocess
import time
logger = logging.getLogger()
import duckdb
import pytest

def delete_existing_db():
    if os.path.exists("warehouse.db"):
        os.remove("warehouse.db")

def run_ingestion_sample_data(sample_data_path) -> float:
    """
    Returns time in seconds that the ingestion process took to run
    """
    print(sample_data_path)
    logger.info("Running ingestion")
    tic = time.perf_counter()
    result = subprocess.run(
        args=[
            "python",
            "-m",
            "equalexperts_dataeng_exercise.ingest",
            sample_data_path,
        ],
        capture_output=True,
    )
    toc = time.perf_counter()
    result.check_returncode()
    print(result)
    return toc - tic

def run_outliers_calculation():
    result = subprocess.run(
        args=["python", "-m", "equalexperts_dataeng_exercise.outliers"],
        capture_output=True,
    )
    result.check_returncode()


def test_check_view_exists():
    sql = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_type='VIEW' AND table_name='outlier_weeks' AND table_schema='blog_analysis';
    """
    run_outliers_calculation()
    con = duckdb.connect("warehouse.db", read_only=True)
    try:
        result = con.execute(sql)
        assert len(result.fetchall()) == 1, "Expected view 'outlier_weeks' to exist"
    finally:
        con.close()


def test_check_view_has_data():
    sql = "SELECT COUNT(*) FROM blog_analysis.outlier_weeks"
    run_outliers_calculation()
    con = duckdb.connect("warehouse.db", read_only=True)
    try:
        result = con.execute(sql)
        assert len(result.fetchall()) > 0, "Expected view 'outlier_weeks' to have data"
    finally:
        con.close()

def test_check_result_view_count_compared_with_distinct_weeks():
    delete_existing_db()
    run_ingestion_sample_data("tests/test-resources/samples-votes.jsonl")
    run_outliers_calculation()
    con = duckdb.connect("warehouse.db", read_only=True)
    view_count = con.execute('select count(*) from blog_analysis.outlier_weeks').fetchall()[0][0]
    assert view_count == 2, "Expected view count based on small sample data" 
