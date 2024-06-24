# pg-recovery-tools
Small utilities I have used for recovering corruption

## trycopy.py

This queries for the page count for a table. It then iterates over the pages in batches. For each page ctids are computed. Then for each ctid batch the `COPY` command is run. If that COPY succeeds the output is written, this is standard output from `COPY`. If the `COPY` fails because a ctid could not be read, the process then breaks the batch of ctids into tenths and tries that smaller batch; this breakdown recurses until the process is operating on single ctids. Finally if the operation fails on a single ctid that failure is logged and optionally written to a CSV file.

The outcome is a row-by-row read of the table to recover as much data as possible. For the rows that could not be read, there is a log (or optional CSV) of what those failed ctids were.

## crc32.py

## xlogfilter.py

xlogfilter is for when xlog replay fails with some error on some specific
table or index, but you want to finish recovery on the rest of the database.

## shiftcorruption.py
shiftcorruption is for a really specific case of corruption where extra
bytes have been inserted into database files moving the rest of the file
forward.