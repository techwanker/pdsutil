class WorkbookHelper:
    def create_sheet_from_cursor(workbook, sheet_name, cursor):
        worksheet = workbook.add_worksheet(sheet_name)
        # process metadata
        colnum = 0
        rownum = 0
        for column_meta in cursor.description:
            worksheet.write(rownum, colnum, column_meta[0])
            worksheet.set_column(colnum, colnum, column_meta[2])
            colnum += 1
        rownum = 1
        for row in cursor:
            colnum = 0
            for coldata in row:
                print ("coldata " + str(rownum) + " " + str(colnum) + " " + str(coldata))
                worksheet.write(rownum, colnum, coldata)
                colnum += 1
            rownum += 1
