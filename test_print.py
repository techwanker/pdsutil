# out_file=open("/tmp/x","w")
# print('"{0}","{1}","{2}"'.format('a',2, False), file=out_file)
# print('"{0}","{1}","{2}"'.format('a',3, False), file=out_file)
import pdsutil.DuplicateFiles2

def time_quick_digest(dir):
    for root, dirs, files in os.walk(dir):
        for file_name in files:
            #  print("\nroot: " + str(root) + "\ndirs: " + str(dirs) + "\n fname: '" + str(file_name) + "'")
            fname = os.path.join(root, file_name)
