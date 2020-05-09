import pandas
import xlsxwriter
from  pdsutil.DbUtil import ConnectionHelper
from pdssr.CdsReportingMetadata import CdsReportingMetadata
import datetime
import random
from pdsutil.Dataset import Dataset
# sql = "select * from etl_sale where etl_file_id = %(ETL_FILE_ID)s"
# binds = {"ETL_FILE_ID" : 201723}
# connection = ConnectionHelper.get_named_connection("current")
#pandas.read_sql(sql, con, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)[source]
#now = datetime.datetime.now()
#sale_dataframe = pandas.read_sql(sql,connection,params=binds)

#print ("dataframe read_sql elapsed: %s" % (datetime.datetime.now() - now))

#DataFrame.to_excel(excel_writer, sheet_name='Sheet1', na_rep='', float_format=None, columns=None, header=True, index=True, index_label=None, startrow=0, startcol=0, engine=None, merge_cells=True, encoding=None, inf_rep='inf', verbose=True)[source]
#workbook = xlsxwriter.Workbook("/tmp/sales_201723.xlsx")
#now = datetime.datetime.now()
#sale_dataframe.to_excel("/tmp/sales_201723.xlsx",sheet_name="sales")

random.seed(3.14159)

list_of_lists = None

def get_data_list_of_lists():
    rows = []
    for i in range (1,20000):
        row = []
        for j in range (1,30):
            row.append(random.randint(0,10000000))
        rows.append(row)
    return rows

def dataframe_list_of_lists():
    a = datetime.datetime.now()
    df = pandas.DataFrame(list_of_lists)
    b = datetime.datetime.now()
    print("dataframe_list_of_lists time %s" % (b -a))
    for row in df.iterrows():
        pass
    c = datetime.datetime.now()
    print("dataframe_list_of_lists iter time %s" % (c - b))
    return a,b

def dataset_list_of_lists():
    a = datetime.datetime.now()
    df = Dataset.from_list_of_lists(list_of_lists,None)
    b = datetime.datetime.now()
    print ("type df %s" % type(df))
    print("dataset_list_of_lists time %s" % (b - a))
    for row in df.rows:
        pass
    c = datetime.datetime.now()

    print("dataset_list_of_lists iter time %s" % (c - b))

    for row in df.rows:
        for col in row:
            pass
    d = datetime.datetime.now()
    print("dataset_list_of_lists iter row col time %s" % (d - c))

    return a, b

list_of_lists = get_data_list_of_lists()
dataframe_list_of_lists()
dataset_list_of_lists()

# #print (sale_dataframe)
# now = datetime.datetime.now()
# html = sale_dataframe.to_html()
# print ("dataframe to html elapsed: %s" % (datetime.datetime.now() - now))
#
# now = datetime.datetime.now()
# for row in sale_dataframe.iterrows():
#     this_row = row
# print ("dataframe iterrows elapsed: %s" % (datetime.datetime.now() - now))
#
# now = datetime.datetime.now()
# rows = []
# for row in sale_dataframe.iterrows():
#     rows.append(row)
# print ("to list of rows elapsed: %s" % (datetime.datetime.now() - now))
#
# now = datetime.datetime.now()
# for row in rows:
#     this_row = row
# print ("elapsed: list of tuples %s" % (datetime.datetime.now() - now))
#
# ""
#
# #workbook.close()
#
# ##
# #record_defs = CdsReportingMetadata().record_defs
#
# #for record_def_name, record_def in record_defs.items():
# #    print ("def %s\n" % record_def)
# #    df = pandas.DataFrame(record_def)
#    print (df)