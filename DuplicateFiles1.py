# https://codereview.stackexchange.com/questions/104420/find-duplicate-files-using-python
__author__ = 'pepoluan'

import os
import hashlib
import collections
from glob import glob
from itertools import chain
import sys
import time


# Global variables
class G:
    OutFormat = 'list'  # Possible values: 'list', 'csv'
    OutFile = None
    NotFoundFileName = "/tmp/not_found.log"
    PermissionErrorFileName = "/tmp/permission_error.log"
    BadSymLinkFileName = "/tmp/bad_sym_link.log"
    FullHashFileName = "/tmp/full_hash.log";
    Operation = ""
    PartialCheckSize = 8192
    FullFileHash = True

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
    def __init__(self, file_name: str):
        self.file_name = file_name
        self._absolute_path = None
        self._real_path = None
        self._mount_point = None
        self._file_stat = None
        self._digest_hash = None
        self._snippet_digest_hash = None


    def stat(self):
        if self._file_stat is None:
            self._file_stat = os.stat(self.realpath())
        return self._file_stat

    def realpath(self):
        if self._real_path is None:
            self._real_path = os.path.realpath(self.file_name)
        return self._real_path

    def mount_point(self):
        path = self._realpath()
        while path != os.path.sep:
            if os.path.ismount(path):
                return path
            path = os.path.abspath(os.path.join(path, os.pardir))
        return path

    def size(self):
        return stat().st_size

    def inode(self):
        return stat().st_inode

    def is_same_inode(self, other ):
        retval = False
        if self.inode() == other.inode() and
            self.mount_point() == other.mount_point():
            retval = True
        return retval



    def equals(self, other):
        retval = None
        while (True)
            if not self.is_same_inode(other):
                retval = False
                break;
            if not self.size() == other.size():
                retval = False
                break


    def digest(self):
        print ("hashing")
        x = None
        if self.digest_hash is None:
            with open(self.realpath(), 'rb') as fin:
                while True:
                    buf = fin.read(self.BLOCK_SIZE)
                    if not buf:
                        break
                    self.hasher.update(buf)
            self.digest_hash = self.hasher.hexdigest()
            print ("hash: " + self.digest_hash)
        return self.digest_hash

    def snipped_digest_hash(self):
        if self._snippet_digest_hash is None:
            with open(self.real_path, 'rb') as fin:
                head_block = fin.read(self.BLOCK_SIZE)
                hash_head = self.hasher.update(head_block).hexdigest()

                hash_tail = hash_head
                tail_block = head_block
                if self.size() - self.BLOCK_SIZE:
                    seek_position = self.size() - self.BLOCK_SIZE - 1
                    fin.seek(seek_position)
                    tail_block = fin.read(self.BLOCK_SIZE)
                    hash_tail = self.hasher.update(head_block).hexdigest()
        self.hasher.update(head_block)
        self.hasher.update(tail_block)
        self._snippet_digest_hash =self.hasher.hexdigest()
        return hash_head, hash_tail, self.snippet_digest_hash



    def isSame(self,other_file_name,computeDigest = False):

        one = open(self.realpath(), "rb")
        two = open(other_file_name, "rb")
        buff1 = None
        buff2 = None
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
                            equals  = False
                            break
                    else:
                        if buff2:
                            equals = False
                            break
                        else:
                            break;
        if computeDigest:
            self.digest_hash = self.hasher.hexdigest()

        end = time.time()
        return equals

Selector = collections.namedtuple('FileHashInfo', 'hash_front hash_rear size_bytes inode')


def printInfo():
    print("minimum file size " + G.MinimumFileSizeBytes)


def dict_filter_by_len(rawdict, minlen=2):
    assert isinstance(rawdict, dict)
    return {k: v for k, v in rawdict.items() if len(v) >= minlen}


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
        print("");

    G.progressCount += 1

    if G.progressCount % G.ProgressPeriod == 0:
        print(p, end='', flush=True)
    if (G.progressCount % (G.ProgressPeriod * 50)) == 0:
        print(p + " " + str(G.progressCount) + " " + G.Operation, flush=True)


def stat(file_name):
    StatInfo = collections.namedtuple("mode")
    info = os.stat(file_name)


def log_permission_error(file_name):
    with open(G.PermissionErrorFileName, "a") as nf:
        nf.write(file_name + "\n")


def get_dupes_by_size(path_list):
    G.Operation = "by size"
    print("path_list " + str(path_list))
    qprint('===== Recursively stat-ing {0}'.format(path_list))
    print()
    processed = set()
    size_dict = {}
    log_progress(reset=True)
    for statpath in path_list:
        print("statpath " + str(statpath))
        file_count = 0
        total_file_count = 0
        uniq_in_path = 0
        qprint('{0}...'.format(statpath), end='')
        print()

        for root, dirs, files in os.walk(statpath):
            for file_name in files:
                #  print("\nroot: " + str(root) + "\ndirs: " + str(dirs) + "\n fname: '" + str(file_name) + "'")
                fname = os.path.join(root, file_name)
                try:

                    log_progress()

                    file_count = 0

                    if fname not in processed:
                        try:
                            file_count += 1
                            total_file_count += 1
                            uniq_in_path += 1

                            fstat = os.stat(fname)
                            fsize = fstat.st_size
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
    dupe_sizes = {(None, sz): list(fset) for sz, fset in size_dict.items() if
                  sz >= G.MinimumFileSizeBytes and len(fset) > 1}
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
    qprint('===== Checking hash of first and last {0} bytes ====='.format(partial_check_size))
    qprint('Processing...', end='', flush=True)
    size_and_hashes = {}
    log_progress("", True)
    for selector, flist in dupes_dict.items():
        fsize = selector[-1]
        for fname in flist:
            try:
                with open(fname, 'rb') as fin:
                    hash_front = hashfunc(fin.read(partial_check_size)).hexdigest()
                    seek_targ = fsize - G.PartialCheckSize - 1
                    if seek_targ > 0:
                        fin.seek(seek_targ)
                        hash_rear = hashfunc(fin.read(partial_check_size)).hexdigest()
                    else:
                        hash_rear = hash_front
                # "size" at rear, so a simple print will still result in a nicely-aligned table
                selector = Selector(hash_front=hash_front, hash_rear=hash_rear, size_bytes=fsize)
                flist = size_and_hashes.get(selector, [])
                flist.append(fname)
                size_and_hashes[selector] = flist
                log_progress(".")
            except PermissionError:
                log_permission_error(fname)
            except FileNotFoundError:
                log_not_found(fname)
    dupe_exact = dict_filter_by_len(size_and_hashes)
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
                print("full hash: " + fname)
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


def output_results(dupes_dict, out_format=G.OutFormat, out_file=G.OutFile):
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


def _main(*argv):
    G.StartPaths = argv[0]

    G.ProgressPeriod = 10000
    dupes = get_dupes_by_size(G.StartPaths)

    G.ProgressPeriod = 10000
    dupes = refine_dupes_by_partial_hash(dupes)

    G.ProgesDotPeriod = 10
    if G.FullFileHash:
        dupes = refine_dupes_by_full_hash(dupes)
    output_results(dupes, out_format=G.OutFormat, out_file=G.OutFile)


if __name__ == '__main__':
    _main("/")
    #    _main(["/common/backups/lenovo-ubuntu/2017-06-18_23-57-13/common/home/jjs"])
