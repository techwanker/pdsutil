from pdsutil.DbUtil import ConnectionHelper, CursorHelper
import sys
import csv


if __name__ == "__main__":
    connection_name = sys.argv[0]
    connection = ConnectionHelper().get_named_connection(connection_name)
    cursor = CursorHelper(connection.cursor())
    rows = cursor.execute(sql,binds)
    quoting_types = [csv.QUOTE_NONNUMERIC, csv.QUOTE_ALL, csv.QUOTE_MINIMAL, csv.QUOTE_NONE]
    def to_csv(self, file, emit_headers: bool = True, dialect: str = "excel", delimiter: str = ",",
               quotechar: str = "'", quoting: str = csv.QUOTE_NONNUMERIC):


        writer = csv.writer(file, dialect="excel",
                            delimiter=',', quotechar='"',
                            quoting=csv.QUOTE_NONNUMERIC)
        if emit_headers:
            writer.writerow(self.column_names)
        for row in self.rows:
            writer.writerow(row)