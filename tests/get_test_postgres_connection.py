# db_url = os.getenv(self.POSTGRES_TEST_URL)

from pdsutil.DbUtil import ConnectionHelper
def get_test_postgres_connection():
    db_url = "postgres:host='localhost' dbname='sales_reporting_db' user='jjs' password='jjs'"  # TODO externalize
    return ConnectionHelper.get_connection(db_url)