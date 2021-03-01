import pytest

from sparkflowtools.models import db


def test_insert_records():
    """Tests db.Dynamo insert_records method"""
    records = [
        {"some_partition_key": "some_partition_value", "some_sort_key": "some_sort_value"},
        {"another_partition_key": "another_partition_value", "another_sort_key": "another_sort_value"}
    ]
    dynamo_db = db.Dynamo()
    dynamo_db.connect(pytest.mock_table_name, pytest.mock_dynamo_resource, pytest.mock_dynamo_table)
    assert pytest.mock_dynamo_table.records == []
    dynamo_db.insert_records(records)
    assert pytest.mock_dynamo_table.records == records


def test_get_record():
    """Tests db.Dynamo get_record method"""
    records = [
        {"some_partition_key": "some_partition_value", "some_sort_key": "some_sort_value"},
        {"another_partition_key": "another_partition_value", "another_sort_key": "another_sort_value"}
    ]
    dynamo_db = db.Dynamo()
    dynamo_db.connect(pytest.mock_table_name, pytest.mock_dynamo_resource, pytest.mock_dynamo_table)
    dynamo_db.insert_records(records)
    keys = {"partition_key_name": "some_partition_key", "sort_key_name": "some_sort_key"}
    table_name = pytest.mock_table_name
    record, _ = dynamo_db.get_record(keys)
    assert record == records[0] and dynamo_db.get_record(keys)
    keys = {"partition_key_name": "some_partition_key"}
    with pytest.raises(ValueError):
        dynamo_db.get_record(keys)


def test_get_records_with_index():
    """Tests db.Dynamo get_records_with_index method"""
    records = [
        {"some_partition_key": "some_partition_value", "some_sort_key": "some_sort_value"},
        {"another_partition_key": "another_partition_value", "another_sort_key": "another_sort_value"}
    ]
    dynamo_db = db.Dynamo()
    dynamo_db.connect(pytest.mock_table_name, pytest.mock_dynamo_resource, pytest.mock_dynamo_table)
    dynamo_db.insert_records(records)
    items, response = dynamo_db.get_records_with_index("some_index", "some_expression", "some_expression_map")


def test_get_db():
    """Tests db.get_db factory function"""
    expected_dbs = [("DYNAMO", db.Dynamo)]
    for expected_db in expected_dbs:
        assert db.get_db(expected_db[0]) == expected_db[1]
    not_available_dbs = ["MYSQL", "SOME_OTHER_DB"]
    for not_available_db in not_available_dbs:
        with pytest.raises(ValueError):
            db.get_db(not_available_db)
