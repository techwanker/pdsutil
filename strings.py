#!/bin/python3
from mmap import mmap, PROT_READ
import re
import sys

def strings(fname, n=4):
    """
        fname  name of file
        n minimun number of [a-zA-Z0-9_] chars
    """
    with open(fname, 'rb') as f, mmap(f.fileno(), 0, prot=PROT_READ) as m:
        for match in re.finditer(('([\x20-\x7E]{%s}[\x20-\x7E]*)' % n).encode(), m):
            string = match.group(0)
            #print("type: " + str(type(string)))
            yield string

def pdf_strings(fname,n=4):
    with open(fname, 'rb') as f, mmap(f.fileno(), 0, prot=PROT_READ) as m:
        for match in re.finditer(('([\x20-\x7E]{%s}[\x20-\x7E]*)Tj' % n).encode(), m):
            string = match.group(0).decode('utf-8')
            string = string[1:-3]
            #print("type: " + str(type(string)))
            yield string
       
if __name__ == '__main__':
    for bstring in pdf_strings(sys.argv[1]):
        string = str(bstring) 
        print(string)
