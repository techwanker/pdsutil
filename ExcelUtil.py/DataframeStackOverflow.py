import pandas
import datetime
import logging
import math
import random
from dateutil.relativedelta import relativedelta
import xlsxwriter

logging.basicConfig(level=logging.INFO)


def get_sales_data(*, num_records, num_custs, num_products, date_from, number_of_months):
    print("number of months %s" % number_of_months)
    sales = {}
    rows = []
    for cust_nbr in range(0, num_custs):
        cust_name = "cust " + "{0:0>3}".format(cust_nbr)
        for month_delta in range(0, number_of_months):
            ship_date = date_from + relativedelta(months=month_delta)
            for product_nbr in (0, num_products):
                product = "product " + str(product_nbr)
                qty = random.randint(0, 20)
                if (qty > 0):
                    key = (cust_name, product, ship_date)
                    shipment = (cust_name, product, ship_date, qty)
                    sales[key] = shipment
    for shipment in sales.values():
        rows.append(shipment)
    return rows


def wtf(b):
    a = None
    for bs in b:
        a = bs


def to_excel(workbook, sheetname, dataframe, column_names):
    worksheet = workbook.add_worksheet(sheetname)
    col_index = 0
    max_widths = [None] * len(dataframe[0])
    for heading in column_names:
        worksheet.write(0, col_index, heading)
        max_widths[col_index] = len(heading)
        col_index += 1
    row_index = 1

    for row in dataframe:
        #max_widths = [None] * len(row)
        col_index = 0
        for datum in row:
            if datum is not None:
                if isinstance(datum, float):
                    if not math.isnan(datum):
                        worksheet.write(row_index, col_index, datum)
                else:
                    worksheet.write(row_index, col_index, datum)
                # print ("len(max_widths) %s col_index %s " % (len(max_widths),col_index))
                if max_widths[col_index] is None or len(str(datum)) > max_widths[col_index]:
                    max_widths[col_index] = len(str(datum))
            # if row_index < 5:
            #     print("r: %s c: %s %s" % (row_index, col_index, datum))
            col_index += 1
        row_index += 1

        col_index = 0
        for width in max_widths:
            worksheet.set_column(col_index, col_index, width + 1)
            col_index += 1

        worksheet.freeze_panes(1,1)
# Get a List of Lists
from_date = datetime.date(2015, 1, 1)
to_date = datetime.date(2017, 7, 1)
matrix = get_sales_data(num_records=3000, num_products=3, date_from=from_date, number_of_months=30, num_custs=1000)

#  Get a dataframe

labels = ["cust_name", "product", "ship_date", "qty"]
dataframe = pandas.DataFrame.from_records(matrix, columns=labels)
print(dataframe)

# get pivot
index = ['cust_name','product']
pivot_table = pandas.pivot_table(dataframe, columns='ship_date', values='qty', index=index)
print(pivot_table)
#column_names = [' '.join(col).strip() for col in pivot_table.columns.values]
#print ("column_names" % column_names)
column_names = []
for col in index:
    column_names.append(col)
for col in pivot_table.columns:
    column_names.append(str(col))
    print ("col is %s" % col)
print ("column_names %s" % column_names)


# convert back to records

records = pivot_table.to_records()

# get excel

output = open("/tmp/crosstab.xslx", "wb")
workbook = xlsxwriter.Workbook(output)
to_excel(workbook, "Cust Product By Month", records, column_names)
workbook.close()
