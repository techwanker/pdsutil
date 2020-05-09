import logging
import unittest
import time

import pdsutil.setup_logging as log_setup
import pdsutil.tests.get_test_postgres_connection as postgres_conn
from pdsutil.DbUtil import DatabaseColumns
import os
from pdsutil.DuplicateFiles3 import FileInfo
import pdsutil.DuplicateFiles3 as dupes
log_setup.setup_logging("../logging.yaml")
logger = logging.getLogger(__name__)


def get_file_path(file_name):
    fdir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(fdir, file_name)

class DatabaseFilesTest(unittest.TestCase):
    path = get_file_path("resources/connections.yaml")

    def test_md5(self):
        print("test_md5")
        path = get_file_path("resources/connections.yaml")
        fi = FileInfo(path)
        start = time.time()
        x = fi.digest()
        end = time.time()
        print (end - start)
        digest = fi.digest()
        self.assertEquals("53ef6c062de7bb2368fd2e6aa1723f47",digest)
        print("test_md5 success")

    def test_comp(self):
        start = time.time()
        f1 = FileInfo(self.path)
        assert(f1.isSame(self.path))
        end = time.time()
        print(end - start)
        print("test_comp success")


    def test_get_partial_hash_key(self):
        result = dupes.get_partial_hash_key("resources/connections.yaml")
        print ("result is %s" % result)

    def test_md5_example(self):
        import hashlib
        m = hashlib.md5()
        m.update("Nobody inspects")
        m.update(" the spammish repetition")
        result = m.digest()
        print(result)
        # '\xbbd\x9c\x83\xdd\x1e\xa5\xc9\xd9\xde\xc9\xa1\x8d\xf0\xff\xe9'
        # >> > m.digest_size
        # 16
        # >> > m.block_size
        # 64



if __name__ == "__main__":

    unittest.main()
