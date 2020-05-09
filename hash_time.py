from pdsutil.DuplicateFiles2 import FileInfo
import time


def digest_file(file_name, block_size):
    a = FileInfo(file_name)
    sz = a.size()
    start = time.time()
    print (a.digest(),block_size)
    end = time.time()
    secs = end -start
    mb_sec = (sz / (1024 * 1024)) / secs
    print ("file: " + file_name  + "mb/sec " + str(mb_sec))

digest_file("/common/home/jjs/Downloads/pycharm-community-2017.1.1.tar",8192)
digest_file("/common/home/jjs/Downloads/pycharm-community-2017.1.1.tar",1024 * 1024)