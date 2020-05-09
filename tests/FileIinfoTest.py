import unittest
import logging
import os
from  pdsutil.DuplicateFiles2 import FileInfo
import time

logger = logging.getLogger(__name__)
class DbIntegrationTestBase(unittest.TestCase):


    def setUp(self):
       pass

    def one(self):
        total_time = 0.0
        file_count = 0
        stop = False
        for root, dirs, files in os.walk("/common/home/jjs"):  #TODO make relative for test
            while not stop:

                for file_name in files:
                    file_count += 1
                    #  print("\nroot: " + str(root) + "\ndirs: " + str(dirs) + "\n fname: '" + str(file_name) + "'")
                    fname = os.path.join(root, file_name)
                    print ("file_count %s fname %s " % (file_count ,fname))
                    start = time.time()
                    try:
                        file_info = FileInfo(fname)
                    #    file_info.snipped_digest_hash()
                    except FileNotFoundError as e:
                        message = fname + " FileNotFoundError " + str(e)
                        print (message)
                        logger.error(message)
                    except OSError as e:
                        logger.error(fname + " OSerror " + str(e))
                    except Exception as e:
                        logger.error(fname + " Exception " + str(e))

                    end = time.time()
                    elapsed = end - start
                    total_time += elapsed
                    if file_count >= 1000:
                        stop = True
                        break
        print ("total time %s file count: %s average %s" % (total_time,file_count, total_time / file_count))

    # TODO need to not test /dev
    # def testZeros(self):
    #     file_info = FileInfo("/dev/zero")
    #     logger.info("zeros " + file_info.digest())

    def tearDown(self):
        logger.info("no teardown necessary")
        # self.connection.execute("DROP DATABASE testdb")