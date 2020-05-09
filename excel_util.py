import pandas
import datetime
import logging
import math
import random
from dateutil.relativedelta import relativedelta
import xlsxwriter





def to_excel(workbook, sheetname, dataframe, column_names):
    """
    
    :param workbook:  An open xlsxwriter workbook
    :param sheetname: The name of the worksheet to create
    :param dataframe: Any object that can be iterated to get rows and then columns in rows
    :param column_names: An iterable collection of strings in order by column
    :return: 
    """
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
