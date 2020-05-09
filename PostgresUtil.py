#!/bin/python
import os
import sys
import json
import collections  # for OrderedDict
import pdsutil.table_json_keys as table_json_keys
import argparse
from pdsutil.DbUtil import ConnectionHelper, CursorHelper


# TODO put in DbUtil




# def get_java_type(column):
#     dataType = column['']
#
#     if (dataType == "VARCHAR" or
#                 dataType == "CHAR" or
#                 dataType == "LONG" or
#                 dataType == "VARCHAR2"):
#         retval = "String"
#     elif dataType == "NUMBER":
#         precision = column['DATA_PRECISION']
#         scale = column['DATA_SCALE']
#         if scale == 0:
#             if precision < 10:
#                 retval = "Integer"
#             elif precision < 19:
#                 retval = "Long"
#             else:
#                 retval = "BigDecimal"
#         else:
#             retval = "BigDecimal"
#     elif dataType == "DATE":
#         retval = "Date"
#     elif dataType == "TIMESTAMP":
#         retval = "Timestamp"
#     else:
#         raise Exception("unhandled type " + dataType)
#     return retval


# def get_model_type(column):
#     # print "column : " + str(column)
#     dataType = column['data_type']
#     length = column['DATA_LENGTH']
#
#     if (dataType == "VARCHAR" or
#                 dataType == "CHAR" or
#                 dataType == "LONG" or
#                 dataType == "VARCHAR2"):
#         retval = "models.CharField(max_length=" + str(length) + ")"
#
#     elif dataType == "NUMBER":
#         precision = column['DATA_PRECISION']
#         scale = column['DATA_SCALE']
#         if scale == 0:
#             if precision < 10:
#                 retval = "models.IntegerField"
#             elif precision < 19:
#                 retval = "models.BigIntegerField"
#             else:
#                 retval = "models.DecimalField(decimal_places=0)"
#         else:
#             retval = "models.DecimalField(max_digits=" + str(precision) + ", decimal_places=" + str(scale) + ")"
#     elif dataType == "DATE":
#         retval = "models.DateTimeField"
#     elif dataType == "TIMESTAMP":
#         retval = "models.DateTimeField"
#     elif dataType == "LONG":
#         retval = "models.TextField"
#     else:
#         raise Exception("unhandled type " + dataType)
#     return retval


def get_pk_columns(conn, table_name):
    # do some shit, get the constraint name
    sql ="""
    SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
    FROM   pg_index i
    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                     AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = %(TABLE_NAME)s::regclass
    AND    i.indisprimary
    """
    cursor = conn.cursor()
    cursor.execute(sql, {"TABLE_NAME": table_name})
    # TODO hack should check if there is more than on
    for row in cursor:
        # print "row " + str(row)
        colName = row[0]
        # print 'PK COL IS ' + str(row['COLUMN_NAME'])
        # return row['COLUMN_NAME']
        cursor.close()
        return colName


def get_line_comma(text, isLast):
    if not isLast:
        return text + ",\n"
    else:
        return text + "\n"


def get_column_list(conn,table_name):
    column_meta_sql = """
    select table_name, column_name, ordinal_position, data_type,
    numeric_precision, numeric_scale, datetime_precision,
    character_maximum_length, is_nullable
    from information_schema.columns
    where table_name like %(TABLE_NAME)s
    order by table_name, ordinal_position

    """
    column_list = []
    column_meta_cursor = conn.cursor()
    column_meta_cursor.execute(column_meta_sql, {"TABLE_NAME": table_name})
    columns = [i[0] for i in column_meta_cursor.description]
    for row in column_meta_cursor:
        # TODO restore in column order then attributes
        column_meta = dict(zip(columns, row))
        sparse_meta = {}

        for k,v  in column_meta.items():

            if k == "is_nullable":
                if v == "YES":
                    sparse_meta["is_nullable"] = True
                else:
                    sparse_meta["is_nullable"] = False
            elif k == "numeric_scale":
                if not column_meta["data_type"] == "integer":
                    sparse_meta["numeric_scale"] = v
            elif not v is None:
                sparse_meta[k] = v

        column_meta[table_json_keys.JAVA_ATTRIBUTE_NAME] = underscore_to_camel_case(column_meta["column_name"])
        column_meta[table_json_keys.PYTHON_MEMBER_NAME] = column_meta["column_name"].lower()
      #  column_meta[table_json_keys.JAVA_TYPE] = get_java_type(column_meta)
      #  column_meta[table_json_keys.MODEL_TYPE] = get_model_type(column_meta)
        column_list.append(sparse_meta)

    column_meta_cursor.close()
    # print "\ngetColumnMeta returning:\n" + str(columnList) + "\n"
    return column_list
def get_select(table_name, column_list):
    code = ""
    code.append('sql = """\n')
    code.append("select * from %s " % table_name)
    code.append("")
def get_persistence_class(table_name, column_list):
    code = ""
    code.append(get_select(table_name, column_list))

def get_table_dictionary(conn, table_name):
    table_dictionary = collections.OrderedDict()
    table_dictionary[table_json_keys.TABLE_NAME] = table_name
    table_dictionary[table_json_keys.PRIMARY_KEY_COLUMN] = get_pk_columns(conn,table_name)  # TODO does not support aggregate
    table_dictionary[table_json_keys.ENTITY_NAME] = underscore_to_camel_case(table_name)
    return table_dictionary


def format_json(tableDictionary):
    #    print json.dumps(tableDictionary, indent=4, sort_keys=True)
    return json.dumps(tableDictionary, indent=4)

def main(arguments):
    if not arguments.out_file is None:
        outfile = open(arguments.out_file,"w")
    else:
        outfile = sys.stdout
    conn = ConnectionHelper().get_named_connection(arguments.connection_name)
    table_dict = get_table_dictionary(conn,arguments.table_name)
    column_list = get_column_list(conn,arguments.table_name)
    table_dict[table_json_keys.COLUMNS] = column_list

    json_text = format_json(table_dict)
    print (json_text)
    outfile.write(json_text)
    outfile.close()
# rows = rows_to_dict_list(cursor)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection_name", required=True,
                        help="name of database connection")
    parser.add_argument("--table_name", required=True, default="test",
                        help="table_name")
    parser.add_argument("--out_file", required=False,
                        help="file_name")
    arguments = parser.parse_args()
    main(arguments)


if __name__ == "__main__":
    main()
