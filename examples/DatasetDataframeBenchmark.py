import pandas
import xlsxwriter
from  pdsutil.DbUtil import ConnectionHelper
from pdssr.CdsReportingMetadata import CdsReportingMetadata
import datetime
import time
import random
from pdsutil.Dataset import Dataset
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
random.seed(3.14159)
MICRO = 1000000

matrices = {}

datasets = {}
dataframes = {}

def get_list_of_lists(row_count, col_count):
    rows = []
    for i in range(0, row_count):
        row = []
        for j in range(0, col_count):
            row.append(random.randint(0, 10000000))
        rows.append(row)
    return rows


def get_dataset_from_list_of_lists(matrix):
    return Dataset.from_list_of_lists(matrix, None)


def get_dataframe_from_list_of_lists(matrix):
    df = pandas.DataFrame(matrix)
    return df


def benchmark_operation(object_description, function, arg):
    function_name = str(function).split(" ")[1]
    start = time.time()
    function(arg)
    end = time.time()
    ela = end - start
    print("description: '%s' function_name: %s elapsed: %s" % (object_description, function_name, ela))
    
def time_op(function):
    start = time.time()
    function
    end = time.time()
    elapsed = end - start
    return elapsed
    
def benchmark_pair(name,function):
    function_name = str(function).split(" ")[1]
    df = dataframes[name]
    ds = datasets[name]
    #logger.info("df type %s ds type %s" % (type(df), type(ds)))
    df_elapsed =  time_op(function(df)) * MICRO
    ds_elapsed =  time_op(function(ds))   * MICRO
    logger.info("description: '%s' function_name: %s dataframe elapsed: %s dataset elapsed %s" %
          (name, function_name, df_elapsed, ds_elapsed))


def iter_rows(dataset):
   for row in dataset.iterrows():
        pass


def iter_rows_len(dataset):
    for row in dataset.iterrows():
        len(row)


def iter_rows_and_columns(dataset):
    for row in dataset.iterrows():
        for col in row:
            pass


def sum_cells(dataset):
    cell_sum = 0
    for row in dataset.iterrows():
        for col in row:
            cell_sum += col


def to_html(dataset):
    dataset.to_html()



def add_matrix(rows, columns):
    matrix = get_list_of_lists(rows, columns)
    name = "%s x %s" % (rows, columns)
    matrices[name] = matrix

add_matrix(20000, 30)
add_matrix(10000, 2)
add_matrix(500, 12)

for k, v in matrices.items():
    dataframes[k] = pandas.DataFrame(v)
    datasets[k] = Dataset.from_list_of_lists(v, None)

for k in datasets:
    df = dataframes[k]
    ds = datasets[k]
    benchmark_pair(k, iter_rows)
    benchmark_pair(k, iter_rows_len)
    benchmark_pair(k, iter_rows_and_columns)
    benchmark_pair(k, sum_cells)
    benchmark_pair(k, to_html)



