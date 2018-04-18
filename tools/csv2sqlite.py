#!/usr/local/bin/python3
#
# A simple CSV to SQLite converter 
#  
#  author : n.nyoman.gandhi@gmail.com 
#  v1 : 18 April 2018
#
# TEST

# Import required packages
import csv
import re
import sqlite3
import argparse
import sys

# Create a function to get header and sample row 
# as inputs for dtypes generator later
def _getHeaderSampleRow(csvfile):
    
    # Load the csv file
    with open(csvfile) as f:
        # Load CSV rows to a list
        reader = csv.reader(f)
        # Get the CSV header
        header = next(reader, None)
        # Skip header/first line
        next(reader, None)
        # Loop through rows, only get row with no blank cells
        for row in reader:
            if '' not in row:
                sample_row = row
                break
    
    return header, sample_row

# Create function for regex matching
def regex_match(cell, REGEX_MAPS):
    returned_key = ''
    dtypes = {}
    for value, pattern in REGEX_MAPS:
        if re.search(pattern, cell):
            returned_key = value
    return returned_key

# Create a function to get data types of the fields
def _getFieldDtype(header, row, REGEX_MAPS):
    
    # Create an empty dict for dtypes
    dtypes = {}

    # Loop through header and cells
    for header, cell in zip(header, row):
        ret_key = regex_match(cell, REGEX_MAPS)
        dtypes[header] = ret_key
    
    return dtypes

# Create a function to load CSV to DB
def csvToDb(raise_flag, csvfile, table_name, db_name, REGEX_MAPS):

    if raise_flag == 'False':
        sys.tracebacklimit = 0 # suppress traceback
        raise Exception('\nHave you defined all the arguments for the script?\n')

    # Get header, sample row
    header, sample_row = _getHeaderSampleRow(csvfile)
    
    # Get data types of the cell based on sample row
    dtypes = _getFieldDtype(header, sample_row, REGEX_MAPS)

    # Define sqlite statements: table creation and value insertion
    stmt_create_table = "CREATE TABLE {} ({})".format(table_name, ",".join('{} {}'.\
                                                                           format(key, value)
                                                                           for key, value
                                                                           in dtypes.items())
                                                     )
    stmt_insert_table = "INSERT INTO {} VALUES ({});".format(table_name, ",".\
                                                             join('?' * len(sample_row)))
    
    # Create sqlite cursor object
    conn = sqlite3.connect('{}.db'.format(db_name))
    c = conn.cursor()
    
    # Create table and insert value to table
    c.execute(stmt_create_table)
    
    with open(csvfile) as f:
        reader = csv.reader(f)
        next(reader, None)
        
        # Insert value to DB
        c.executemany(stmt_insert_table, reader)
    
    # Close cursor connection
    conn.commit()
    conn.close()

    # Return table schema
    schema = dtypes
    return schema

# create an argument parser function: argument_list
def argument_list(REGEX_MAPS):
    
    # Create a parser object: parser
    parser = argparse.ArgumentParser(description="Define function arguments")
    parser.add_argument('-f', '--file', help="input csv file")
    parser.add_argument('-d', '--db', help="SQLITE db filename")
    parser.add_argument('-t', '--table', help="SQLITE table name")
    args = parser.parse_args()
    
    # Handle case where there is no argument fed to the script
    #print(len(sys.argv))
    raise_flag = 'True'

    if len(sys.argv) != 7:
        print()
        print('Script Aborted! See below notes:')
        print()
        parser.print_help()
        raise_flag = 'False'
    
    # Return args objects
    csvfile = args.file
    db = args.db
    table = args.table
    
    return raise_flag, csvfile, db, table

# Create main function
def main():
    REGEX_MAPS = [
        ('BOOLEAN', r'^(true|false)|(yes|no)'), # sqlite does not have storage for boolean type
                                                # but keep it anyway
        ('VARCHAR', r'^[a-z\sA-Z]+$'),
        ('INTEGER', r'^-?\d+$'),
        ('FLOAT', r'^-?\d+\.{1}\d+$'), # escape the dot to catch float
        ('TEXT', r'[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])'), # sqlite does not have a storage class 
                                                                          # for storing date/times
                                                                          # but keep it anyway
    ]
    raise_flag, csvfile, db, table = argument_list(REGEX_MAPS)
    #print(raise_flag, csvfile, db, table)
    table_schema = csvToDb(raise_flag, csvfile, table, db, REGEX_MAPS)
    print("DB Loading Success!" + '\n'
          "DB Name : {}.db".format(db) + '\n'
          "Table Name : {}".format(table) + '\n'
          "Table Schema: {}".format(table_schema)
         )

# Begin main
if __name__ == '__main__':
    main()