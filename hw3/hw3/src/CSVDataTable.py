import csv  # Python package for reading and writing CSV files.

# You MAY have to modify to match your project's structure.
from src import DataTableExceptions
from src import CSVCatalog as CSVCatalog


import json

max_rows_to_print = 10


class CSVTable:
    # Table engine needs to load table definition information.
    __catalog__ = CSVCatalog.CSVCatalog()

    def __init__(self, t_name, load=True):
        """
        Constructor.
        :param t_name: Name for table.
        :param load: Load data from a CSV file. If load=False, this is a derived table and engine will
            add rows instead of loading from file.
        """

        self.__table_name__ = t_name

        # Holds loaded metadata from the catalog. You have to implement  the called methods below.
        self.__description__ = None
        if load:
            self.__load_info__()  # Load metadata
            self.__rows__ = None
            self.__load__()  # Load rows from the CSV file.

            # Build indexes defined in the metadata. We do not implement insert(), update() or delete().
            # So we can build indexes on load.
            self.__build_indexes__()
        else:
            self.__file_name__ = "DERIVED"

    def __load_info__(self):
        """
        Loads metadata from catalog and sets __description__ to hold the information.
        :return:
        """
        # recitation
        self.__description__ = self.__catalog__.get_table(self.__table_name__)
        # print(json.dumps(self.__description__.to__json(), indent=2))
        pass

    def __get_file_name__(self):
        return self.__description__.file_name

    def __add_row__(self, new_r):
        if self.__rows__ is None:
            self.__rows__ = []
        self.__rows__.append(new_r)

    # Load from a file and creates the table and data.
    def __load__(self):
        fn = self.__get_file_name__()
        try:
            with open(fn, "r") as csvfile:
                # CSV files can be pretty complex. You can tell from all of the options on the various readers.
                # The two params here indicate that "," separates columns and anything in between " ... " should parse
                # as a single string, even if it has things like "," in it.
                reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')

                # Get the names of the columns defined for this table from the metadata.
                column_names = self.__get_column_names__()

                # Loop through each line (well dictionary) in the input file.
                for r in reader:
                    # Only add the defined columns into the in-memory table. The CSV file may contain columns
                    # that are not relevant to the definition.
                    projected_r = self.project([r], column_names)[0]
                    self.__add_row__(projected_r)

        except IOError as e:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_file,
                message="Could not read file = " + fn)

    def __get_column_names__(self):
        cn = self.__description__.columns
        result = [k.column_name for k in cn]
        return result
        # print("Columns = ", result)

    def __get_row_count__(self):
        if self.__rows__ is not None:
            return len(self.__rows__)
        else:
            return 0

    def get_row_list(self):
        return self.__rows__

    def __str__(self):
        """
        You can do something simple here. The details of the string returned depend on what properties you
        define on the class. So, I cannot provide a simple implementation.
        :return:
        """
        return self.__table_name__

    def __get_index_info__(self, index_name):
        if self.__description__.indexes is not None:
            idx_def = self.__description__.indexes
            for d in idx_def:
                if d.index_name == index_name:
                    return d

    def __get_index_value__(self, row, index_name):
        idx_elements = []
        idx_info = self.__get_index_info__(index_name)
        columns = idx_info.column_names
        if not isinstance(columns, list):
            columns = [columns]
        for c in columns:
            idx_elements.append(row[c])
        result = "_".join(idx_elements)

        return result

    def __build_index__(self, index_name):
        if self.__description__.indexes is None:
            return
        new_index = {}
        for r in self.__rows__:
            idx_value = self.__get_index_value__(r, index_name)
            idx_entry = new_index.get(idx_value, [])
            idx_entry.append(r)
            new_index[idx_value] = idx_entry

        return new_index

    def __build_indexes__(self):
        self.__indexes__ = {}
        defined_indexes = self.__description__.indexes
        if defined_indexes is None:
            return
        for x in defined_indexes:
            new_idx = self.__build_index__(x.index_name)
            self.__indexes__[x.index_name] = new_idx

    def __get_access_path__(self, tmp):
        """
        Returns best index matching the set of keys in the template.
        Best is defined as the most selective index, i.e. the one with the most distinct index entries.
        An index name is of the form "colname1_colname2_coluname3" The index matches if the
        template references the columns in the index name. The template may have additional columns, but must contain
        all of the columns in the index definition.
        :param tmp: Query template.
        :return: Index or None
        """
        if self.__indexes__ is None:
            return None, None
        else:
            result = None
            count = None
        if tmp is None:
            return None

        tmp_set = set(tmp)
        if not isinstance(self.__description__.indexes, list):
            idx_list = [self.__description__.indexes]
        else:
            idx_list = self.__description__.indexes

        if self.__description__.indexes is None or not self.__description__.indexes:
            return None, None

        for the_idx in idx_list:
            if not isinstance(the_idx.column_names, list):
                col_names = [the_idx.column_names]
            else:
                col_names = the_idx.column_names
            columns = set(col_names)
            if columns.issubset(tmp_set):
                if result is None:
                    result = the_idx.index_name
                    count = len(self.__indexes__[result])
                else:
                    if count < len(self.__indexes__[result]):
                        result = the_idx.index_name
                        count = len(self.__indexes__[result])
        return result, count


    def matches_template(self, row, t):
        """
        :param row: A single dictionary representing a row in the table.
        :param t: A template
        :return: True if the row matches the template.
        """

        # Basically, this means there is no where clause.
        if t is None:
            return True

        try:
            c_names = list(t.keys())
            for n in c_names:
                if row[n] != t[n]:
                    return False
            else:
                return True
        except Exception as e:
            raise (e)

    def project(self, rows, fields):
        """
        Perform the project. Returns a new table with only the requested columns.
        :param fields: A list of column names.
        :return: A new table derived from this table by PROJECT on the specified column names.
        """
        try:
            if fields is None:  # If there is not project clause, return the base table
                return rows  # Should really return a new, identical table but am lazy.
            else:
                result = []
                for r in rows:  # For every row in the table.
                    tmp = {}  # Not sure why I am using range.
                    for j in range(0, len(fields)):  # Make a new row with just the requested columns/fields.
                        v = r[fields[j]]
                        tmp[fields[j]] = v
                    else:
                        result.append(tmp)  # Insert into new table when done.

                return result

        except KeyError as ke:
            # happens if the requested field not in rows.
            raise DataTableExceptions.DataTableException(-2, "Invalid field in project")

    def __find_by_template_scan__(self, t, fields=None, limit=None, offset=None):
        """
        Returns a new, derived table containing rows that match the template and the requested fields if any.
        Returns all row if template is None and all columns if fields is None.
        :param t: The template representing a select predicate.
        :param fields: The list of fields (project fields)
        :param limit: Max to return. Not implemented
        :param offset: Offset into the result. Not implemented.
        :return: New table containing the result of the select and project.
        """

        if limit is not None or offset is not None:
            raise DataTableExceptions.DataTableException(-101, "Limit/offset not supported for CSVTable")

        # If there are rows and the template is not None
        if self.__rows__ is not None:

            result = []

            # Add the rows that match the template to the newly created table.
            for r in self.__rows__:
                if self.matches_template(r, t):
                    result.append(r)

            result = self.project(result, fields)
        else:
            result = None

        return result

    def __find_by_template_index__(self, t, idx, fields=None, limit=None, offset=None):
        """
        Find using a selected index
        :param t: Template representing a where clause/
        :param idx: Name of index to use.
        :param fields: Fields to return.
        :param limit: Not implemented. Ignore.
        :param offset: Not implemented. Ignore
        :return: Matching tuples.
        """
        idx_name = idx
        #idx_info = self.__get_index_info__(idx_name)
        #idx_columns = idx_info['columns']
        key_values = self.__get_index_value__(t, idx_name)
        the_index = self.__indexes__[idx_name]
        tmp_result = the_index.get(key_values, None)

        if tmp_result:
            result = []
            for r in tmp_result:
                if self.matches_template(r, t):
                    result.append(r)
            result = self.project(result, fields)
        else:
            result = None

        return result

    def find_by_template(self, t, fields=None, limit=None, offset=None):
        # 1. Validate the template values relative to the defined columns.
        # 2. Determine if there is an applicable index, and call __find_by_template_index__ if one exists.
        # 3. Call __find_by_template_scan__ if not applicable index.
        #return self.__find_by_template_scan__(t, fields=fields)
        if t is not None:
            access_index, count = self.__get_access_path__(list(t.keys()))
        else:
            access_index = None
        if access_index is None:
            return self.__find_by_template_scan__(t, fields=fields, limit=None, offset=None)
        else:
            result = self.__find_by_template_index__(t, access_index, fields, limit, offset)
            return result

    def insert(self, r):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Insert not implemented"
        )

    def delete(self, t):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Delete not implemented"
        )

    def update(self, t, change_values):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Updated not implemented"
        )

    def __choose_scan_probe_table__(self, right_r, on_fields):
        left_path, left_count = self.__get_access_path__(on_fields)
        right_path, right_count = right_r.__get_access_path__(on_fields)

        if left_path is None and right_path is None:
            return self, right_r
        elif left_path is None and right_path is not None:
            return self, right_r
        elif left_path is not None and right_path is None:
            return right_r, self
        elif right_count < left_count:
            return self, right_r
        else:
            return right_r,self

    def __table_from_rows__(self, name, rows):
        result = CSVTable(name, load=False)
        if rows is None:
            return result
        new_rows = []
        for i in range(0, len(rows)):
            new_rows.append(rows[i])
        result.__rows__ = new_rows
        return result

    def __join_rows__(self, l, r, on_fields):
        result_rows = []
        for lr in l:
            on_template = self.__get_on_template__(lr, on_fields)
            for rr in r:
                if self.matches_template(rr, on_template):
                    new_r = {**lr, **rr}
                    result_rows.append(new_r)
        return result_rows

    def __get_on_template__(self, row, fields):
        """
        :param row: a dictionary
        :param fields: a list
        :return: a dictionary
        """
        template = {}
        for f in fields:
            if row[f]:
                template[f] = row[f]
        return template

    def __get_sub_where_template__(self, where):
        cols = self.__get_column_names__()
        sub = {}
        if not where:
            return None
        for k in where:
            if k in cols:
                sub[k] = where[k]
        return sub

    def join(self, right_r, on_fields, where_template=None, project_fields=None):
        """
        Implements a JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.
        :param left_r: The left table, or first input table
        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: List of dictionary elements, each representing a row.
        """

        # If not optimizations are possible, do a simple nested loop join and then apply where_clause and
        # project clause to result.
        #
        # At least two vastly different optimizations are be possible. You should figure out two different optimizations
        # and implement them.
        #
        scan_t, probe_t = self.__choose_scan_probe_table__(right_r, on_fields)
        scan_sub_template = scan_t.__get_sub_where_template__(where_template)
        probe_sub_template = probe_t.__get_sub_where_template__(where_template)

        if scan_t is not self:
            print("Swapping scan and probe tables.")

        optimize = True

        if optimize:
            print("Before pushdown, scan table size is = ", len(scan_t.get_row_list()))
            print("Attempting to pushdown WHERE template = ", json.dumps(where_template))
            scan_rows = scan_t.find_by_template(scan_sub_template)
            print("After pushdown, scan table size is = ", len(scan_rows))
        else:
            scan_rows = scan_t.get_row_list()

        join_result = []

        # left_rows_processed = 0

        for l_r in scan_rows:
            on_template = scan_t.__get_on_template__(l_r, on_fields)
            if probe_sub_template is not None:
                probe_where = {**on_template, **probe_sub_template}
            else:
                probe_where = on_template

            current_right_rows = probe_t.find_by_template(probe_where)

            if current_right_rows is not None and len(current_right_rows) > 0:
                new_rows = self.__join_rows__([l_r], current_right_rows, on_fields)
                join_result.extend(new_rows)

            #            left_rows_processed += 1
            #            if left_rows_processed %10 == 0:
            #                print("JOIN has processed ", left_rows_processed, " rows from left table.")

            final_rows = []
            for r in join_result:
                if self.matches_template(r, where_template):
                    r = self.project([r], fields=project_fields)
                    final_rows.append(r[0])

            join_result = self.__table_from_rows__(
                "JOIN(" + self.__table_name__ + "," + right_r.__table_name__ + ")", final_rows
            )

            return join_result

