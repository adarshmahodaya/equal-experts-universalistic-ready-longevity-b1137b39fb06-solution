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

def count_rows_in_data_file():
    with open("uncommitted/votes.jsonl", "r", encoding="utf-8") as data:
        return sum(1 for _ in data)

def count_rows_in_sample_data_file(sample_data_path):
    with open(sample_data_path, "r", encoding="utf-8") as data:
        return sum(1 for _ in data)

#Test function starts

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
    run_ingestion_sample_data("tests/test-resources/samples-votes.jsonl")
    con = duckdb.connect("warehouse.db", read_only=True)
    result2 = con.execute(sql)
    table_count_after = result2.fetchall()[0][0]
    con.close()
    assert (
        table_count_after == table_count_before + count_rows_in_sample_data_file("tests/test-resources/samples-votes.jsonl")
    ), "Count should have increased by number of files in sample jsonl"

def test_check_corrupt_sample_input_data():

    sql = "SELECT COUNT(*) FROM blog_analysis.votes"
    run_ingestion()
    con = duckdb.connect("warehouse.db", read_only=True)
    result1 = con.execute(sql)    
    table_count_before = result1.fetchall()[0][0]
    con.close()
    run_ingestion_sample_data("tests/test-resources/samples-votes-corrupt.jsonl")
    con = duckdb.connect("warehouse.db", read_only=True)
    result2 = con.execute(sql)
    table_count_after = result2.fetchall()[0][0]
    con.close()
    assert (
        table_count_after == table_count_before
    ), "Count should not increase, as the data load for the new batch should fail "

def test_check_extra_fields_in_one_record_sample_input_data():

    sql = "SELECT COUNT(*) FROM blog_analysis.votes"
    output = run_ingestion_sample_data("tests/test-resources/samples-votes-extraColumn.jsonl")
    print(output)
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.execute(sql)
    table_count = result.fetchall()[0][0]
    con.close()
    assert (
        table_count  ==  count_rows_in_sample_data_file("tests/test-resources/samples-votes-extraColumn.jsonl") - 1
    ), "Code should ignore the record with extra column and load data"


def test_check_missingNotNullColumn_sample_input_data():
    sql = "SELECT COUNT(*) FROM blog_analysis.votes"
    output = run_ingestion_sample_data("tests/test-resources/samples-votes-missingANotNullColumn.jsonl")
    print(output)
    con = duckdb.connect("warehouse.db", read_only=True)
    result = con.execute(sql)
    table_count = result.fetchall()[0][0]
    con.close()
    assert (
        table_count  ==  0
    ), "Code should not load data for the batch as there is a invalid record"
