import logging
import os
import subprocess
import time

import duckdb
import pytest

logger = logging.getLogger()


@pytest.fixture(autouse=True)
def delete_existing_db():
    if os.path.exists("warehouse.db"):
        os.remove("warehouse.db")


def run_ingestion() -> float:
    """
    Returns time in seconds that the ingestion process took to run
    """
    logger.info("Running ingestion")
    tic = time.perf_counter()
    result = subprocess.run(
        args=[
            "python",
            "-m",
            "equalexperts_dataeng_exercise.ingest",
            "uncommitted/votes.jsonl",
        ],
        capture_output=True,
    )
    toc = time.perf_counter()
    result.check_returncode()
    return toc - tic

def run_ingestion_sample_data() -> float:
    """
    Returns time in seconds that the ingestion process took to run
    """
    logger.info("Running ingestion")
    tic = time.perf_counter()
    result = subprocess.run(
        args=[
            "python",
            "-m",
            "equalexperts_dataeng_exercise.ingest",
            "tests/test-resources/samples-votes.jsonl",
        ],
        capture_output=True,
    )
    toc = time.perf_counter()
    result.check_returncode()
    print(result)
    return toc - tic

def run_ingestion_corrupt_data() -> float:
    """
    Returns time in seconds that the ingestion process took to run
    """
    logger.info("Running ingestion")
    tic = time.perf_counter()
    result = subprocess.run(
        args=[
            "python",
            "-m",
            "equalexperts_dataeng_exercise.ingest",
            "tests/test-resources/samples-votes-corrupt.jsonl",
        ],
        capture_output=True,
    )
    toc = time.perf_counter()
    result.check_returncode()
    print(result)
    return toc - tic

def test_check_table_exists():
    run_ingestion()
    sql = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_type LIKE '%TABLE' AND table_name='votes' AND table_schema='blog_analysis';
    """
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.sql(sql)
    assert len(result.fetchall()) == 1, "Expected table 'votes' to exist"


def count_rows_in_data_file():
    with open("uncommitted/votes.jsonl", "r", encoding="utf-8") as data:
        return sum(1 for _ in data)

def count_rows_in_sample_data_file():
    with open("tests/test-resources/samples-votes.jsonl", "r", encoding="utf-8") as data:
        return sum(1 for _ in data)

def test_check_correct_number_of_rows_after_ingesting_once():
    sql = "SELECT COUNT(*) FROM blog_analysis.votes"
    time_taken_seconds = run_ingestion()
    assert time_taken_seconds < 10, "Ingestion solution is too slow!"
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.execute(sql)
    count_in_db = result.fetchall()[0][0]
    assert (
        count_in_db <= count_rows_in_data_file()
    ), "Expect only as many entries in votes as lines in the data file"


def test_check_correct_number_of_rows_after_ingesting_twice():
    sql = "SELECT COUNT(*) FROM blog_analysis.votes"
    for _ in range(2):
        run_ingestion()
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.execute(sql)
    count_in_db = result.fetchall()[0][0]
    assert (
        count_in_db <= count_rows_in_data_file()
    ), "Expect only as many entries in votes as lines in the data file"

def test_check_sample_input_data():

    sql = "SELECT COUNT(*) FROM blog_analysis.votes"
    run_ingestion()
    con = duckdb.connect("warehouse.db", read_only=True)
    result1 = con.execute(sql)    
    table_count_before = result1.fetchall()[0][0]
    con.close()
    run_ingestion_sample_data()
    con = duckdb.connect("warehouse.db", read_only=True)
    result2 = con.execute(sql)
    table_count_after = result2.fetchall()[0][0]
    con.close()
    assert (
        table_count_after == table_count_before + count_rows_in_sample_data_file()
    ), "Count should have increased by number of files in sample jsonl"

def test_check_corrupt_sample_input_data():

    sql = "SELECT COUNT(*) FROM blog_analysis.votes"
    run_ingestion()
    con = duckdb.connect("warehouse.db", read_only=True)
    result1 = con.execute(sql)    
    table_count_before = result1.fetchall()[0][0]
    con.close()
    run_ingestion_corrupt_data()
    con = duckdb.connect("warehouse.db", read_only=True)
    result2 = con.execute(sql)
    table_count_after = result2.fetchall()[0][0]
    con.close()
    assert (
        table_count_after == table_count_before
    ), "Count should have increased by 1"