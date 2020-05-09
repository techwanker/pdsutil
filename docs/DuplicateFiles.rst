Duplicate Files
===============

Very efficient routine to look for duplicate files and to generate deduplication 
scripts.

Approach
--------
Makes three passes.

Length
******
The first pass finds files of the same size in bytes.

Partial Hash
************
A partial hash is generated on the first and last 8k of data.

Full Hash
*********     if size > block_size:
            hash_tail = hash_head
            tail_block = head_block
FIles that are the same size and have the same partial hash have a full hash 
calculated.

Options
-------
Full hashes can be 

Files
=====

Database
--------
A sqllite database is created

Duplicate Files
---------------

duplicate_files.log

File Not Found
--------------

Files can disappear during processing, files that disappear logged to

not_found.log

Permission Error
----------------
permission_error.log

Bad Symbolic Link
-----------------
bad_sym_link.log

Full Hash
---------

full_hash.log";
