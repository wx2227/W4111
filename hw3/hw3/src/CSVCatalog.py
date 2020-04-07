import pymysql
import csv
import json


class ColumnDefinition:
    """
    Represents a column definition in the CSV Catalog.
    """

    # Allowed types for a column.
    column_types = ("text", "number")

    def __init__(self, column_name, column_type="text", not_null=False):
        """

        :param column_name: Cannot be None.
        :param column_type: Must be one of valid column_types.
        :param not_null: True or False
        """
        if (column_name is None) or \
            (column_type not in ColumnDefinition.column_types) or \
            not isinstance(not_null, bool):
            raise ValueError("Wrong column name!")

        self.column_name = column_name
        self.column_type = column_type
        self.not_null = not_null


    def __str__(self):
        pass

    def to_json(self):
        """

        :return: A JSON object, not a string, representing the column and it's properties.
        """
        result = {
            "column_name": self.column_name,
            "column_type": self.column_type,
            "not_null" : self.not_null
        }
        return result

class IndexDefinition:
    """
    Represents the definition of an index.
    """
    index_types = ("PRIMARY", "UNIQUE", "INDEX")

    def __init__(self, index_name, index_type, column_names):
        """

        :param index_name: Name for index. Must be unique name for table.
        :param index_type: Valid index type.
        """
        self.index_name = index_name
        if index_type not in IndexDefinition.index_types:
            raise ValueError("Wrong index type!")
        if len(column_names) == 0:
            raise ValueError("No column names!")

        self.index_type = index_type
        self.column_names = column_names

    def to_json(self):
        result = {
            "index_name": self.index_name,
            "type": self.index_type,
            "columns": self.column_names
        }
        return result


class TableDefinition:
    """
    Represents the definition of a table in the CSVCatalog.
    """

    def __init__(self, t_name=None, csv_f=None, column_definitions=None, index_definitions=None,
                 cnx=None, load=False):
        """

        :param t_name: Name of the table.
        :param csv_f: Full path to a CSV file holding the data.
        :param column_definitions: List of column definitions to use from file. Cannot contain invalid column name.
            May be just a subset of the columns.
        :param index_definitions: List of index definitions. Column names must be valid.
        :param cnx: Database connection to use. If None, create a default connection.
        """
        dbhost = "localhost"
        dbport = 3306
        dbname = "CSVCatalog"
        dbuser = "dbuser"
        dbpw = "dbuser"
        self.cnx = pymysql.connect(
            host=dbhost, db=dbname, user=dbuser, password=dbpw, port=dbport,
            charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor)
        self.table_name = t_name
        self.columns = None
        self.indexes = None

        if not load:
            if t_name is None or csv_f is None:
                raise ValueError("No table name.")

            if not self.__is_file__(csv_f):
                raise ValueError("File doesn't exist.")

            self.file_name = csv_f

            self.__save_core_definition__()

            if column_definitions is not None:
                for c in column_definitions:
                    if self.__check_column_in_file__(c) is False:
                        raise ValueError("Defined column not in csv file.")
                    if self.__check_duplicated_columns__(c) is True:
                        raise ValueError("Duplicated column definitions.")
                    self.add_column_definition(c)

            if index_definitions is not None:
                for idx in index_definitions:
                    self.define_index(idx)

        else:
            self.__load_core_definition__()
            self.__load_columns__()
            self.__load_indexes__()

    def __is_file__(self, fn):
        try:
            with open(fn, "r") as a_file:
                return True
        except:
            return False

    def __check_column_in_file__(self, c):
        with open(self.file_name, newline='') as file:
            reader = csv.reader(file)
            first_line = None
            for row in reader:
                first_line = row
                break
        if c.column_name in first_line:
            return True
        else:
            return False

    def __check_duplicated_columns__(self, c):
        if not self.columns:
            return False
        b = 0
        for col in self.columns:
            if col.column_name == c.column_name:
                b = 1
        return b == 1

    def __load_columns__(self):
        q = "select * from csvtablecolumns where table_name='" + self.table_name + "'"
        result = self.__run_q__(self.cnx, q, None, fetch=True, commit=True)
        for r in result:
            new_cd = ColumnDefinition(r['column_name'], r['type'], r['not_null'] == 1)
            if self.columns is None:
                self.columns = []
            self.columns.append(new_cd)

    def __load_indexes__(self):
        q = "select * from csvtableindexes where table_name='" + self.table_name + \
            "' GROUP BY index_name, id, table_name, kind, column_name ORDER BY key_column_order"
        result = self.__run_q__(self.cnx, q, None, fetch=True, commit=True)
        in_names = []
        new_in = None
        for r in result:
            if not r['index_name'] in in_names:
                if self.indexes is None:
                    self.indexes = []
                else:
                    self.indexes.append(new_in)
                new_in = IndexDefinition(r['index_name'], r['kind'], r['column_name'])
                # self.indexes.append(new_in)
                in_names.append(new_in)
            else:
                new_in.column_names.append(r['column_name'])
        if new_in:
            self.indexes.append(new_in)

    def __save_core_definition__(self):
        q = "insert into csvtables values(%s, %s)"
        self.__run_q__(self.cnx, q, (self.table_name, self.file_name), fetch=False, commit=True)

    def __load_core_definition__(self):
        q = "select * from csvtables where table_name='" + self.table_name + "'"
        result = self.__run_q__(self.cnx, q, None, fetch=True, commit=True)
        self.file_name = result[0]['file_name']

    def __str__(self):
        return json.dumps(self.to_json(), indent=2)

    @classmethod
    def load_table_definition(cls, cnx, table_name):
        """

        :param cnx: Connection to use to load definition.
        :param table_name: Name of table to load.
        :return: Table and all sub-data. Read from the database tables holding catalog information.
        """

        pass

    def __save_column_definition__(self, c):
        q = "insert into csvtablecolumns values(%s, %s, %s, %s)"
        self.__run_q__(self.cnx, q, (self.table_name, c.column_name, c.column_type, c.not_null),
                                fetch=False, commit=True)

    def add_column_definition(self, c):
        """
        Add a column definition.
        :param c: New column. Cannot be duplicate or column not in the file.
        :return: None
        """
        self.__save_column_definition__(c)
        if self.columns is None:
            self.columns = []
        self.columns.append(c)

    def drop_column_definition(self, c):
        """
        Remove from definition and catalog tables.
        :param c: Column name (string)
        :return:
        """
        q = "DELETE FROM csvtablecolumns WHERE table_name = '"+self.table_name+"' AND column_name = '"+c+"'"
        self.__run_q__(self.cnx, q, fetch=False, commit=True)
        self.columns.remove(c)

    def to_json(self):
        """

        :return: A JSON representation of the table and it's elements.
        """
        result = {
            "table_name": self.table_name,
            "file_name": self.file_name
        }
        if self.columns is not None:
            result['columns'] = []
            for c in self.columns:
                result['columns'].append(c.to_json())

        if self.indexes is not None:
            result['indexes'] = []
            for idx in self.indexes:
                result['indexes'].append(idx.to_json())

        return result

    def define_primary_key(self, columns):
        """
        Define (or replace) primary key definition.
        :param columns: List of column values in order.
        :return:
        """
        if not columns:
            return

        cols = []
        for c in self.columns:
            cols.append(c.column_name)
        if not all(x in cols for x in columns):
            raise ValueError("No column definition for primary key.")

        for c in self.columns:
            if c.column_name in columns:
                c.not_null = True

        for col in columns:
            for c in self.columns:
                if c.column_name == col and c.not_null is not True:
                    c.not_null = True
                    self.drop_column_definition(c.column_name)
                    self.add_column_definition(c)
        if self.indexes:
            for i in self.indexes:
                if i.type == 'PRIMARY':
                    self.indexes.remove(i)
                    self.drop_index("primary_key")
        p = IndexDefinition('primary_key', 'PRIMARY', columns)
        if not self.indexes:
            self.indexes = []
        self.indexes.append(p)
        self.__save_index_definition__("primary_key", columns, 'PRIMARY')

    def __save_index_definition__(self, i_name, cols, k):
        q = "insert into csvtableindexes (table_name, column_name, kind, key_column_order, index_name) " + \
            " values(%s, %s, %s, %s, %s)"
        for i in range(0,len(cols)):
            v = (self.table_name, cols[i], k, str(i), i_name)
            self.__run_q__(self.cnx, q, v, fetch=False, commit=True)

    def define_index(self, index_name, kind="INDEX", columns=None):
        """
        Define or replace and index definition.
        :param index_name: Index name, must be unique within a table.
        :param columns: Valid list of columns.
        :param kind: One of the valid index types.
        :return:
        """
        if columns is None:
            raise ValueError("Index column error.")
        if self.indexes:
            for idx in self.indexes:
                if idx.index_name == index_name:
                    self.drop_index(index_name)
                    print("Index redefined.")
                    break
        self.__save_index_definition__(index_name, columns, kind)
        if self.indexes is None:
            self.indexes = []
        new_idx = IndexDefinition(index_name, kind, columns)
        self.indexes.append(new_idx)

    def drop_index(self, index_name):
        """
        Remove an index.
        :param index_name: Name of index to remove.
        :return:
        """
        q = "DELETE FROM csvtableindexes WHERE table_name = '"+self.table_name+"' AND index_name = '"+index_name+"'"
        self.__run_q__(self.cnx, q, fetch=False, commit=True)
        for i in self.indexes:
            if i.index_name == index_name:
                self.indexes.remove(i)
                break

    def describe_table(self):
        """
        Simply wraps to_json()
        :return: JSON representation.
        """
        return self.to_json()

    def __run_q__(self, cnx, q, args=None, fetch=False, commit=True):
        if args is None:
            cursor = cnx.cursor()
            cursor.execute(q)
        else:
            cursor = cnx.cursor()
            cursor.execute(q, args)
        if fetch:
            result = cursor.fetchall()
        else:
            result = None
        if commit is True:
            cnx.commit()
        return result


'''
    def __load_index_definitions__(self):
        q = "select * from csvtableindexes where table_name = '" + self.table_name +"' order by key_column_order"
        result = dffutils.run_q(self.cnx, q, fetch=False, commit=True)
        if result and len(result) > 0:
            self.index_definitions = {}

            for r in result:
                this_idx = self.index_definitions.get(r['index_name'], None)
                if this_idx is None:
                    this_idx = {
                        "index_name": r['index_name'],
                        "kind": r['kind'],
                        "columns":
                    }
'''

class CSVCatalog:

    def __init__(self, dbhost="localhost", dbport=3306, dbname="CSVCatalog",
                 dbuser="dbuser", dbpw="dbuser", debug_mode=None):
        self.cnx = pymysql.connect(
            host=dbhost, db=dbname, user=dbuser, password=dbpw, port=dbport,
            charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor)

    def __str__(self):
        pass

    def create_table(self, table_name, file_name, column_definitions=None, primary_key_columns=None):
        '''
        q = "select * from csvtables where table_name = '"+table_name+"'"
        result = self.__run_q__(self.cnx, q, fetch=True, commit=True)
        print(if result)
        if result:
            raise ValueError("Duplicated tables!")
        '''
        result = TableDefinition(table_name, file_name, column_definitions=column_definitions, cnx=self.cnx)
        result.define_primary_key(primary_key_columns)
        return result

    def drop_table(self, table_name):
        q = "DELETE FROM csvtables WHERE table_name='"+table_name+"'"
        self.__run_q__(self.cnx, q, fetch=False, commit=True)
        q = "DELETE FROM csvtablecolumns WHERE table_name='"+table_name+"'"
        self.__run_q__(self.cnx, q, fetch=False, commit=True)
        q = "DELETE FROM csvtableindexes WHERE table_name='"+table_name+"'"
        self.__run_q__(self.cnx, q, fetch=False, commit=True)

    def get_table(self, table_name):
        """
        Returns a previously created table.
        :param table_name: Name of the table.
        :return:
        """
        result = TableDefinition(table_name, load=True, cnx=self.cnx)
        return result

    def __run_q__(self, cnx, q, args=None, fetch=False, commit=True):
        if args is None:
            cursor = cnx.cursor()
            cursor.execute(q)
        else:
            cursor = cnx.cursor()
            cursor.execute(q, args)
        if fetch:
            result = cursor.fetchall()
        else:
            result = None
        if commit is True:
            cnx.commit()
        return result













