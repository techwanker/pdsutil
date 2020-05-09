# https://codereview.stackexchange.com/questions/104420/find-duplicate-files-using-python
__author__ = 'TechWanker'
# TODO Allow for file_info per volume
# TODO Compare Accross volumes
# TODO OSError: [Errno 6] No such device or address: '/common/home/jjs/.local/share/ubuntu-amazon-default/SingletonSocket'

import os
import hashlib
import collections
from glob import glob
from itertools import chain
import sys
import time
import sqlite3
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# Global variables
class G:
    OutFormat = 'list'  # Possible values: 'list', 'csv'
    OutFile = "/tmp/duplicate_files.log"
    DuplicateFiles = None
    NotFoundFileName = "/tmp/not_found.log"
    PermissionErrorFileName = "/tmp/permission_error.log"
    BadSymLinkFileName = "/tmp/bad_sym_link.log"
    FullHashFileName = "/tmp/full_hash.log";
    Operation = ""
    PartialCheckSize = 8192
    FullFileHash = True
    connection = None

    MinimumFileSizeBytes = 1 * 1024 * 1024
    ProgLinePeriod = 50  # Number of files to process before reporting progress
    ProgressPeriod = 50  # Number of files to process before printing a dot
    FullBlockSize = 1024 * 1024
    Quiet = False
    progressCount = 0

    HashFunc = hashlib.md5

    def __init__(self, *StartPaths):
        self.StartPaths = StartPaths


class FileInfo:
    BLOCK_SIZE = 8192
    hasher = hashlib.md5()

    def __init__(self, file_name: str, directory: str = None):
        self._file_name = file_name
        self._hash = None
        self._absolute_path = None
        self._real_path = None
        self._mount_point = None
        self._file_stat = None
        self._digest_hash = None
        self._snippet_digest_hash = None
        if directory:
            self._real_path = os.path.join(directory, file_name)

    def __hash__(self):
        if self._hash == None:
            self._hash = self._file_name.__hash__()
        return self._hash

    def stat(self):
        if self._file_stat is None:
            self._file_stat = os.stat(self.realpath())
        return self._file_stat

    def realpath(self) -> str:
        if self._real_path is None:
            self._real_path = os.path.realpath(self._file_name)
        return self._real_path

    def mount_point(self):
        path = self.realpath()
        while path != os.path.sep:
            if os.path.ismount(path):
                return path
            path = os.path.abspath(os.path.join(path, os.pardir))
        return path

    def size(self):
        return self.stat().st_size

    def inode(self):
        return self.stat().st_ino

    def is_same_inode(self, other):
        retval = False
        if self.inode() == other.inode() and self.mount_point() == other.mount_point():
            retval = True
        return retval

    def digest(self) -> str:
        """
        
        :return: hexadecimal string md5 hash
        """
        logger.info("hashing " + self.realpath())
        if self._digest_hash is None:
            if self._file_name.startswith("/run/" or self._file_name.startswith("/dev/")):
                logger.warning("should not try to digest " + self._file_name)
            else:
                try:
                    with open(self.realpath(), 'rb') as fin:
                        while True:
                            buf = fin.read(self.BLOCK_SIZE)
                            if not buf:
                                break
                            self.hasher.update(buf)
                    self._digest_hash = self.hasher.hexdigest()
                    # print("hash: " + self._digest_hash)
                except Exception as e:
                    logger.error("digest " + self.realpath() + " " + str(e))
        return self._digest_hash

    def snipped_digest_hash(self):
        """
        Hash the first BLOCK_SIZE block and the last BLOCK_SIZE block
        This is a quick way of narrowing down suspects by file size.
        :return: 
        """
        head_block = None
        tail_block = None
        if self._snippet_digest_hash is None:
            with open(self.realpath(), 'rb') as fin:
                head_block = fin.read(self.BLOCK_SIZE)
                self.hasher.update(head_block)
                hash_head = self.hasher.hexdigest()
                hash_tail = hash_head
                tail_block = head_block
                if self.size() >= self.BLOCK_SIZE:
                    seek_position = self.size() - self.BLOCK_SIZE - 1
                    fin.seek(seek_position)
                    tail_block = fin.read(self.BLOCK_SIZE)
                    self.hasher.update(tail_block)
                    hash_tail = self.hasher.hexdigest()
        self.hasher.update(head_block)
        self.hasher.update(tail_block)
        self._snippet_digest_hash = self.hasher.hexdigest()
        return hash_head, hash_tail, self._snippet_digest_hash

    def isSame(self, other_file_name, computeDigest=False):
        chunk = 1024 * 1024
        start = time.time()
        equals = True
        chunks = 0
        with open(self.realpath(), "rb") as one:
            with open(other_file_name, "rb") as two:
                while True:
                    buff1 = one.read(chunk)
                    buff2 = two.read(chunk)
                    if computeDigest:
                        self.hasher.update(buff1)
                    if buff1:
                        if buff2:
                            if not buff1 == buff2:
                                equals = False
                                break
                        else:
                            equals = False
                            break
                    else:
                        if buff2:
                            equals = False
                            break
                        else:
                            break
        if computeDigest:
            self.digest_hash = self.hasher.hexdigest()

        end = time.time()
        return equals

    def file_identifier(self):
        return ((self.inode()), self.mount_point())


class FileInfoPersistence:
    def __init__(self, run_number):
        t = time.time()
        self.connection = sqlite3.connect("/tmp/file_info_" + str(t) + ".db", detect_types=sqlite3.PARSE_DECLTYPES)
        self.cursor = self.connection.cursor()
        self.file_info_insert_cursor = None

    def get_binds(self, file_info):

        file_info_map = {}
        file_info_map["file_name"] = file_info.realpath()
        file_info_map["digest"] = file_info.digest()
        file_info_map["mod_date"] = None  # TODO  file_info.mod_date()
        file_info_map["mount_point"] = file_info.mount_point()
        file_info_map["partial_digest"] = file_info._snippet_digest_hash
        file_info_map["size"] = file_info.size()
        # print ("binds: " + str(file_info_map))
        return file_info_map

    def file_info_insert(self, file_info):
        sql = """
        insert into file_info (
              file_name,
              digest,
               mod_date,
              partial_digest,
              size )
        values (
           :file_name,
           :digest,
           :partial_digest,
           :mod_date,
           :size
        )
        """
        try:
            file_info_map = self.get_binds(file_info)
            # if self.file_info_insert_cursor is None:
            #     self.file_info_insert_cursor = self.connection.cursor()
            # #     self.file_info_insert_cursor.execute(sql, file_info_map)
            # # else:
            # self.file_info_insert_cursor.execute(sql,file_info_map)
        except Exception as e:
            # TODO update file may have disappearer
            logger.warning(e)

    def file_info_update(self, file_info):
        return None  # TODO restore

        sql = """
           update file_info 
           set   digest = :digest,
                 mod_date = :mod_dt,
                 partial_digest = :partial_digest,
                 size = :size
           where 
              file_name = %(file_name)s # TODO consider mount points
           """

        file_info_map = self.get_binds(file_info)
        self.cursor.execute(sql, file_info_map)

    def init_db(self):

        self.cursor.execute("PRAGMA foreign_keys = on")  # must be enabled with every session
        file_info_sql = \
            """ 
            create table file_info
             (file_info_id integer primary key,
              file_name    varchar(256) not null,
              digest       varchar(32),
              partial_digest varchar2(32),
              mod_date     date,    --# TODO make not null
              uid          integer, -- TODO make not null
              gid          integer, -- TODO make not null
              mod          integer, -- TODO make not null
              mount_point  varchar2(64),
              size         long not null
              --run_number   int -- not null
             )
            """
        self.cursor.execute(file_info_sql)

    def commit(self):
        self.connection.commit()


PartialHashKey = collections.namedtuple('FileHashInfo', 'hash_partial size_bytes')
SizeKey = collections.namedtuple("FileSizeId", "size inode")

PartialHashKey = collections.namedtuple('FileHashInfo', 'hash_partial size_bytes')

def dict_filter_by_len(rawdict, minlen=2):

    retval = {}
    assert isinstance(rawdict, dict)
    logger.warning("dict_filter_by_len")
    logger.warning("len before " + str(len(rawdict)))
    # return {k: v for k, v in rawdict.items() if len(v) >= minlen}
    for k, v in rawdict.items():
        print("k %s len %s " % (str(k), len(v)))
        if len(v) >= 2:
            retval[k] = v
            logger.warning("k %s len %s" % (k, len(v)))
        else:
            logger.warning("k %s filtered " % str(k))
    logger.warning("len after " + str(len(retval)))
    return retval

def get_partial_hash_key(file_name, block_size=8192):
    """
    Hash the first BLOCK_SIZE block and the last BLOCK_SIZE block
    This is a quick way of narrowing down suspects by file size.
    :return: 
    """
    head_block = None
    tail_block = None


    with open(file_name, 'rb') as fin:
        hasher = hashlib.md5()
        hasher.hexdigest()
        head_block = fin.read(block_size)
        hasher.update(head_block)
        hash_head = hasher.hexdigest()

        x = hashlib.md5()
        x.update(head_block)
        hash_head_2 = hasher.hexdigest()

        assert (hash_head == hash_head_2)

        size = os.stat(file_name).st_size
        hash_tail = hash_head
        tail_block = head_block
        if size >= block_size:
            seek_position = size - block_size - 1
            fin.seek(seek_position)
            tail_block = fin.read(block_size)
        hasher.update(tail_block)
    partial_hash_key = hasher.hexdigest()
    partial_hash_key = PartialHashKey(hash_partial=partial_hash_key,
                                      size_bytes=size)
    return partial_hash_key


def printInfo():
    print("minimum file size " + str(G.MinimumFileSizeBytes))





def qprint(*args, **kwargs):
    if not G.Quiet:
        print(*args, **kwargs)


def log_not_found(file_name):
    if os.path.islink(file_name) and not os.path.exists(file_name):
        with open(G.BadSymLinkFileName, "a") as bs:
            # qprint("bad symlink: " + file_name)
            bs.write(file_name + "\n")
    else:
        with open(G.NotFoundFileName, "a") as nf:
            # qprint("no such file: " + file_name)
            nf.write(file_name + "\n")


def log_progress(p=".", reset=False):
    if reset:
        G.progressCount = 0
        print("")

    G.progressCount += 1

    if G.progressCount % G.ProgressPeriod == 0:
        print(p, end='', flush=True)
    if (G.progressCount % (G.ProgressPeriod * 50)) == 0:
        print(p + " log_progress  " + str(G.progressCount) + " " + G.Operation, flush=True)


def log_permission_error(file_name):
    with open(G.PermissionErrorFileName, "a") as nf:
        nf.write(file_name + "\n")


# def get_size_key(file_name):
#     stat = os.stat_file_name
#     size = stat.st_size
#     inode = stat.st_ino
#     key = collections.nSizeKey = collections.namedtuple("FileSizeId", "size inode")"size inode")
#     return


def get_dupes_by_size(path_list):
    G.Operation = "by size"
    print("path_list " + str(path_list))
    qprint('===== Recursively stat-ing {0}'.format(path_list))
    print()
    processed = set()
    size_dict = {}
    log_progress(reset=True)
    total_file_count = 0
    logger.info("get_dupes_by_size: %s" % (path_list))
    for statpath in path_list:
        print("statpath " + str(statpath))
        #file_count = 0
        #total_file_count = 0
        uniq_in_path = 0
        qprint('{0}...'.format(statpath), end='')
        print()
        stop = False
        for root, dirs, files in os.walk(statpath):
            #print("\nroot: %s" % root)
            if not stop:
                for file_name in files:
                    total_file_count += 1

                    if total_file_count > 100000:
                        stop = True;
                        break;
                    #  print("\nroot: " + str(root) + "\ndirs: " + str(dirs) + "\n fname: '" + str(file_name) + "'")
                    fname = os.path.join(root, file_name)
                    try:

                        log_progress()

                        file_count = 0

                        if fname not in processed:
                            try:
                                file_count += 1
             #                   total_file_count += 1
                                uniq_in_path += 1

                                fsize = os.stat(fname).st_size

                                flist = size_dict.get(fsize, set())
                                flist.add(fname)
                                size_dict[fsize] = flist

                                processed.add(fname)
                            except FileNotFoundError:  # file may have disappeard or be a bad symlink
                                log_not_found(fname)
                            except PermissionError as p:
                                log_permission_error(fname)

                    except:
                        print('\nException on ', fname)
                        raise
        qprint(uniq_in_path)
    qprint('\nTotal files: ', len(processed))
    # dupe_sizes = {sz: list(fset) for sz, fset in size_dict.items() if
    #               sz >= G.MinimumFileSizeBytes and len(fset) > 1}
    ordered_size_dict = collections.OrderedDict(sorted(size_dict.items()))
    print("reducing: %s " % (str(len(ordered_size_dict))))
    dupe_sizes  = dict_filter_by_len(size_dict)
    qprint('Dupes: ', len(dupe_sizes))
    return dupe_sizes






# def hashing_strategy(file_name:str, strategy, blocksize, hasher):


# def refine_dupes(dupes_dict, hashing_strategy, block_size, hashfunc):
#     hashes = {}
#     for selector, flist in dupes_dict.items():
#         fsize = selector[-1]
#         for name in flist:
#             hasher = hashfunc()
#             with open(fname, 'rb') as fin:
#                 result = hashing_strategy(fin, strategy, blocksize, hasher)
#                 flist = fullhashes.get(result, [])
#                 flist.append(fname)
#                 hashes[result] = flist
#     dupe_exact = dict_filter_by_len(hashes)
#     return dupe_exact

def refine_dupes_by_partial_hash(dupes_dict, partial_check_size=G.PartialCheckSize, hashfunc=G.HashFunc):
    G.Operation = "partial hash"
    assert isinstance(dupes_dict, dict)
    qprint('===== Checking hash of    first and last {0} bytes ====='.format(partial_check_size))
    qprint('Processing...', end='', flush=True)
    logger.warn("dupes by size size %s" % len(dupes_dict))
    size_and_hashes = {}
    log_progress("", True)
    # TODO consider just reading and comparing for 16k or less
    start = time.time()
    for file_size, flist in dupes_dict.items():

        #logger.warn("481 file_size %s %s" % (str(file_size), type(file_size)))
        if file_size > G.PartialCheckSize:
            logger.warning("partial hash - file size %s flist %s" % (file_size, len(flist)))
            # fsize = selector[-1]
            for fname in flist:
                try:
                    logger.info("examining " + fname)

                    partial_hash_key = get_partial_hash_key(fname)
                    print ("hash: %s fname %s" % (partial_hash_key, fname))
                    flist = size_and_hashes.get(partial_hash_key, [])
                    flist.append(fname)
                    size_and_hashes[partial_hash_key] = flist
                    # G.persistence.file_info_update(file_info)
                    log_progress(".")
                except PermissionError:
                    log_permission_error("by partial hash " + fname)
                except FileNotFoundError:
                    log_not_found("by partial hash " + fname)
        else:
            logger.warning("refine_dupes_partial_hash skipping %s %s " % (file_size, len(flist)))
    logger.warning("done")
    end = time.time()
    logger.warning("elapsed %s" % (end - start))
    logger.warning("sizes and hashes length " + str(len(size_and_hashes)))
    dupe_exact = dict_filter_by_len(size_and_hashes,'x')
    qprint('\nDupes: ', len(dupe_exact))
    return dupe_exact


def refine_by_file_info(dupes_dict, block_size=G.FullBlockSize, hashfunc=G.HashFunc):
    """
    By definition two files with the same inode are the same file in two directories, no deduping is necessary
    
    Map<Digest,Map[Inode],List<FileInfo>
    :param dupes_dict: 
    :param block_size: 
    :param hashfunc: 
    :return: 
    """
    G.Operation = "full hash"
    log_progress(reset=True)
    assert isinstance(dupes_dict, dict)
    qprint('===== Checking full hashes of Dupes')
    qprint('Processing...', end='', flush=True)
    fullhashes = {}
    digest_inode_fileinfo = {}
    i = 0
    for selector, flist in dupes_dict.items():
        i += 1
        print("files: %d  %s" % (i,flist))
        sz = selector[-1]  # Save size so we can still inform the user of the size
        inode_file_info = {}
        try:

            for fname in flist:
                if fname.startswith("/run/"):
                    logger.error("should not be processing " + fname)
                else:
                    finfo = FileInfo(fname)
                    ino = finfo.inode()
                    l = inode_file_info.get(ino, [])
                    l.append(finfo)
                    inode_file_info[ino] = l

            for file_id, inode_list in inode_file_info.items():
                finfo = inode_list[0]
                logger.info("digesting " + finfo.realpath())
                digest_list = []
                digest_list.append(finfo)
                digest = finfo.digest()
                for fileInfo in inode_list[1:]:
                    digest_list = fullhashes.get(digest, [])
                    digest_list.append(fileInfo)
                    G.DuplicateFiles.write(digest + " " + str(fileInfo.inode()) + " " + fileInfo.realpath() + "\n")
                    fullhashes[digest] = digest_list
                    logger.info("k %s len v %s" % (file_id, len(digest_list)))
                    log_progress()
        except PermissionError:
            log_permission_error("refine " + fname)
        except FileNotFoundError:
            log_not_found("refine " + fname)
    logger.info("about to filter")
    dupe_exact = dict_filter_by_len(fullhashes)
    qprint('\nDupes: ', len(dupe_exact))
    return dupe_exact


def refine_dupes_by_full_hash(dupes_dict, block_size=G.FullBlockSize, hashfunc=G.HashFunc):
    G.Operation = "full hash"
    log_progress(reset=True)
    assert isinstance(dupes_dict, dict)
    qprint('===== Checking full hashes of Dupes')
    qprint('Processing...', end='', flush=True)
    fullhashes = {}
    for selector, flist in dupes_dict.items():
        print("files: " % flist)
        sz = selector[-1]  # Save size so we can still inform the user of the size
        try:
            for fname in flist:
                if fname.startswith("/run/"):
                    logger.error("should not be processing " + fname)
                else:
                    logger.info("hashing " + fname)
                    # print("full hash: " + fname)
                    hasher = hashfunc()
                    with open(fname, 'rb') as fin:
                        while True:
                            buf = fin.read(block_size)
                            if not buf:
                                break
                            hasher.update(buf)
                    # "size" at rear, so a simple print will still result in a nicely-aligned table
                    digest_size_tuple = (hasher.hexdigest(), sz)
                    flist = fullhashes.get(digest_size_tuple, [])
                    flist.append(fname)
                    fullhashes[digest_size_tuple] = flist
                    log_progress()
        except PermissionError:
            log_permission_error(fname)
        except FileNotFoundError:
            log_not_found(fname)
    dupe_exact = dict_filter_by_len(fullhashes)
    qprint('\nDupes: ', len(dupe_exact))
    return dupe_exact


def emit_results(dupes_dict, out_format=G.OutFormat, out_file=G.OutFile):
    assert isinstance(dupes_dict, dict)
    kiys = [k for k in dupes_dict]
    kiys.sort(key=lambda x: x[-1])

    if out_file is not None:
        qprint('Writing result in "{0}" format to file: {1} ...'.format(out_format, out_file), end='')
    else:
        qprint()

    if out_format == 'list':
        for kiy in kiys:
            flist = dupes_dict[kiy]
            print('-- {0}:'.format(kiy), file=out_file)
            flist.sort()
            for fname in flist:
                print('   {0}'.format(fname), file=out_file)
    elif out_format == 'csv':
        print('"Ord","Selector","FullPath"', file=out_file)
        order = 1
        for kiy in kiys:
            flist = dupes_dict[kiy]
            flist.sort()
            for fname in flist:
                print('"{0}","{1}","{2}"'.format(order, kiy, fname), file=out_file)
                order += 1

    if out_file is not None:
        qprint('done.')


def output_results(dupes_dict, out_format=G.OutFormat, out_file=G.OutFile):
    # assert isinstance(dupes_dict, dict)
    # kiys = [k for k in dupes_dict]
    # kiys.sort(key=lambda x: x[-1])
    print('"Ord","Selector","FullPath"', file=out_file)
    order = 1
    for digest, inode_map in dupes_dict.items():
        print("%s %s", (digest, inode_map))
        # digest_list = []
        # for inode, inode_list in inode_map.items():
        #     digest_list.extend(inode_list)
        #     # digest_list.sort()
        # for fname in digest_list:
        #     print('"{0}","{1}"'.format(order, key, fname), file=out_file)
        #     order += 1


def _main(*argv):
    G.StartPaths = argv
    G.persistence = FileInfoPersistence(8)

    G.persistence.init_db()

    G.DuplicateFiles = open("/tmp/duplicate_files", "w")

    G.ProgressPeriod = 10000
    dupes = get_dupes_by_size(G.StartPaths)

    G.ProgressPeriod = 10000
    dupes = refine_dupes_by_partial_hash(dupes)
    #
    G.ProgessDotPeriod = 10
    if G.FullFileHash:
        #dupes = refine_dupes_by_full_hash(dupes)
        dupes = refine_by_file_info(dupes)
    G.OutFile = open("/tmp/output","w")
    output_results(dupes, out_format=G.OutFormat, out_file=G.OutFile)
    G.DuplicateFiles.close()
    G.OutFile.close()
    # G.persistence.commit()


if __name__ == '__main__':
    _main("/common")
    #    _main(["/common/backups/lenovo-ubuntu/2017-06-18_23-57-13/common/home/jjs"])
