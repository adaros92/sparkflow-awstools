import boto3
import logging
from abc import ABC, abstractmethod

from sparkflowtools.utils import dynamo_db

SUPPORTED_DBS = {"DYNAMO"}


class DB(ABC):

    def __init__(self, name: str = None):
        super().__init__()
        self.name = name
        self.connection = None

    @abstractmethod
    def connect(self, **kwargs) -> None:
        """Connects to the database"""
        pass

    @abstractmethod
    def insert_records(self, records: list) -> list:
        """Inserts the list of records into the database table

        :param records a list of database records to insert
        :returns a list of failed records
        """
        pass

    @abstractmethod
    def delete_records(self, records: list) -> list:
        """Deletes a list of record identifiers from the database table

        :param records IDs of records to delete
        :returns a list of failed records
        """
        pass


class Dynamo(DB):

    DB_TYPE = "DYNAMO"

    def __init__(self):
        super().__init__()
        self.connection = None
        self.table = None
        self.table_name = None

    def connect(self, table_name: str, resource: boto3.resource = None, table_object=None):
        """Retrieves the DynamoDB boto3 resource as the connection object to submit requests to"""
        self.table, self.connection = dynamo_db.get_dynamo_table(table_name, resource, table_object)
        self.table_name = table_name
        return self

    def insert_records(self, records: list) -> list:
        """Inserts a list of records into the DynamoDB table by the provided name

        :param records a list of records to insert into the Dynamo table
        :returns a list of failed records during insertion
        """
        failed = []
        for record in records:
            try:
                dynamo_db.write_item_to_dynamodb(self.table_name, record, self.connection, self.table)
            except Exception as e:
                logging.warning("could not insert {0} into {1}".format(record, self.table_name))
                logging.exception(e)
                failed.append(record)
        return failed

    def delete_records(self, records: list) -> list:
        """Deletes records from DynamoDB as identified by the keys contained in the given records list

        :param records a list of DynamoDB keys to delete from the table
        :returns a list of failed records during deletion
        """
        failed = []
        for record in records:
            key_to_delete = record.get("Key")
            try:
                dynamo_db.delete_item_by_key(self.table_name, key_to_delete, self.connection, self.table)
            except Exception as e:
                logging.warning("could not delete {0} from {1}".format(key_to_delete, self.table_name))
                logging.exception(e)
                failed.append(record)
        return failed

    def get_record(self, keys: dict):
        """Retrieves one record from a DynamoDB table by the provided name

        :param keys a dictionary containing the partition/sort keys that identifies the record to retrieve
        """
        return dynamo_db.get_item_from_dynamodb_table(self.table_name, keys, self.connection, self.table)

    def get_records_with_index(self, index: str, expression: str, expression_map: dict):
        """Retrieves multiple records from a DynamoDB table by the provided name using the given secondary index

        :param index the name of the secondary index to use for the Dynamo query
        :param expression the query expression to use in the DynamoDB query
        :param expression_map key value pairs of identifiers in the expression to values they should take
        """
        return dynamo_db.get_items_with_index(
            self.table_name, index, expression, expression_map, self.connection, self.table)

    def record_exists(self, keys: dict):
        """Checks whether a record identified by the given keys exists in the database table or not

        :param keys a dictionary containing the partition/sort keys that identifies the record to retrieve
        :returns True if the record exists and False if not
        """
        if not self.get_record(keys):
            return False
        return True


def get_db(db_type: str, supported_db_types: set = None):
    """Retrieve a database class to instantiate from the given name and set of supported types

    :param db_type the type of database object to retrieve
    :param supported_db_types a set of supported DB types that can be retrieved
    :returns a reference to a DB class that can be instantiated by the client
    """
    if not supported_db_types:
        supported_db_types = SUPPORTED_DBS
    if db_type not in supported_db_types:
        raise ValueError(
            "the given DB type of {0} is not one of the supported DBs: {1}".format(db_type, supported_db_types)
        )
    type_to_class_map = {"DYNAMO": Dynamo}
    return type_to_class_map[db_type]
