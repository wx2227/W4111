import os
cwd = os.getcwd()
print(cwd)

os.chdir(cwd[:-4])

from src import CSVCatalog
from src import CSVDataTable
import time
import json


def cleanup():
    """
    Deletes previously created information to enable re-running tests.
    :return: None
    """
    cat = CSVCatalog.CSVCatalog()
    cat.drop_table("people")
    cat.drop_table("batting")
    cat.drop_table("teams")


def print_test_separator(msg):
    print("\n")
    lot_of_stars = 20*'*'
    print(lot_of_stars, '  ', msg, '  ', lot_of_stars)
    print("\n")


def test_find_by_template():

    cleanup()
    print_test_separator("Starting test_find_by_template")

    cat = CSVCatalog.CSVCatalog()
    cds = []

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))
    cds.append(CSVCatalog.ColumnDefinition("birthCity", "text"))
    cds.append(CSVCatalog.ColumnDefinition("birthCountry", "text"))
    cds.append(CSVCatalog.ColumnDefinition("throws", column_type="text"))


    t = cat.create_table(
        "people",
        "/Users/Yan/Desktop/hw3/data/People.csv",
        cds)
    t.define_index("id_idx", "INDEX", ['nameLast'])
    print("People table metadata = \n", json.dumps(t.describe_table(), indent=2))

    people_tbl = CSVDataTable.CSVTable("people")
    print("Loaded people table = \n", people_tbl)

    tries = 1000
    start_time = time.time()
    templ = { "nameLast": "Williams"}
    print("Starting test on find using indexed field, templ = ", json.dumps(templ))
    for i in range(0, tries):
        result = people_tbl.find_by_template(templ, ['playerID', 'nameLast', 'nameFirst'])
        if i == 0:
            print("Sample result = ", json.dumps(result))
    end_time = time.time()
    print("Elapsed time for ", tries, "lookups = ", end_time-start_time)

    tries = 1000
    start_time = time.time()
    templ = {"nameFirst": "Ted"}
    print("\n\nStarting test on find using NON-indexed field, tmpl = ", json.dumps(templ))
    for i in range(0, tries):
        result = people_tbl.find_by_template(templ, ['playerID', 'nameLast', 'nameFirst'])
        if i == 0:
            print("Sample result = ", json.dumps(result))
    end_time = time.time()
    print("Elapsed time for ", tries, "lookups = ", end_time - start_time)

    print_test_separator("Complete test_find_by_template")


test_find_by_template()