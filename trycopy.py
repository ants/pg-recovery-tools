import csv
import logging
import sys
import time
import gzip
try:
    from StringIO import StringIO ## for Python 2
except ImportError:
    from io import StringIO ## for Python 3
from optparse import OptionParser

import psycopg2

parser = OptionParser(usage="usage: %prog [options] connstr srctable outfilebasepath",
                      description="""Tries to copy out as much data as possible from a broken table.""")
parser.add_option("-c", "--csv", dest="csv",
                  help="store broken ctids into csv FILE", metavar="FILE")
parser.add_option("-l", "--log", dest="log",
                  help="store logs into FILE", metavar="FILE")
parser.add_option("-p", "--lpmax", dest="lpmax", type="int", default=300,
                  help="maximum number of linepointers in a page")
parser.add_option("-b", "--batch", dest="batch", type="int", default=100000,
                  help="Starting batch size in pages")
# the failure this is trying to hedge against is something like running out of disk for the output file
# if we run out of disk this allows for the trycopy to be restarted
# it would generally be restarted at the last successful page batch written
# this will avoid rescanning the previously extracted pages pages
parser.add_option("-s", "--startpage", dest="startpage", type="int", default=0,
                  help="The page to start with, typically the last value logged as 'complete' during an aborted run")

(options, args) = parser.parse_args()

default_page_range = options.batch
max_linepointers_per_page = options.lpmax
start_page = options.startpage

if len(args) != 3:
    parser.print_usage()
    sys.exit(1)

connstring = args[0]
tablename = args[1]
outfilebasepath = args[2]

root = logging.getLogger()
fmt = logging.Formatter('[%(asctime)-15s] %(message)s')
if options.log:
    fh = logging.FileHandler(options.log)
    fh.setFormatter(fmt)
    root.addHandler(fh)
sh = logging.StreamHandler()
sh.setFormatter(fmt)
sh.setLevel(logging.WARNING)
root.addHandler(sh)
root.setLevel(logging.INFO)

log = logging.getLogger('trycopy')
log.setLevel(logging.INFO)

if options.csv:
    csvfd = open(options.csv, "a")
    csvwriter = csv.writer(csvfd)
else:
    csvwriter = None

def new_connection():
    global conn, cur
    while True:
        try:
            log.info("Trying to get a new connection")
            conn = psycopg2.connect(connstring)
            break
        except psycopg2.Error:
            time.sleep(1)

    cur = conn.cursor()


class Stats(object):
    def __init__(self):
        self.success = 0
        self.fail = 0

    def __str__(self):
        return "%s success, %s fail" % (self.success, self.fail)


new_connection()
cur.execute("SELECT pg_relation_size(%s::regclass)/8192 AS num_pages", (tablename,))
total_pages, = cur.fetchone()

log.warn("Processing relation %s", tablename)


# given a base path, the start and end page counts, returns a string that is the file name to write to
def compute_filename(base_path, start, end, table_name):
    return "%s/%s.trycopy.%s-to-%s.out.gz" % (base_path, table_name, start, end)


def copy_range(ctids):
    query = StringIO()
    query.write("COPY (SELECT * FROM ")
    query.write(tablename)
    query.write(" WHERE ctid = ANY('{")
    query.write(", ".join(ctids))
    query.write("}'::tid[])) TO STDOUT")
    while True:
        try:
            buf = StringIO()
            cur.copy_expert(query.getvalue(), buf)
            fd.write(buf.getvalue().encode("utf-8"))
            stats.success += cur.rowcount
            return
        except psycopg2.Error as e:
            # set this to always log the exception so we know what happened
            logging.warning("Handling exception in copy_range, this should not be fatal")
            logging.warning(e, exc_info=True)
            if isinstance(e, psycopg2.InterfaceError):
                logging.warning("Refreshing DB connection")
                new_connection()
            else:
                try:
                    logging.warning("Rolling back query")
                    conn.rollback()
                except psycopg2.Error as x:
                    logging.warning("Refreshing DB connection after rollback attempt")
                    new_connection()
            if len(ctids) > 1:
                batch_size = max(1, len(ctids) / 10)
                log.info("Query error encountered: %s, bisecting into batches of %d" % (e, batch_size))
                for start in range(0, len(ctids), batch_size):
                    copy_range(ctids[start:start + batch_size])
                return
            else:
                stats.fail += 1
                log.error("Failed row %s" % ctids[0])
                if csvwriter is not None:
                    page, lp = ctids[0][2:-2].split(",")
                    row = "%0.3f" % time.time(), page, lp, str(e)
                    csvwriter.writerow(row)
                return
        finally:
            buf.close()


stats = Stats()
log.warn("Total pages: %d" % total_pages)
last_complete = 0.
for start in range(start_page, total_pages, default_page_range):
    if float(start) / total_pages - last_complete > 0.01:
        last_complete = float(start) / total_pages
        log.warning("%2f%% complete, %s", (last_complete * 100), stats)
    end = min(total_pages, start + default_page_range)
    log.warning("Start processing pages in batch from %i to %i", start, end)
    #the file name gets incremented with the batch count
    #gzip compression should generally work well with the output from COPY
    fd = gzip.open(compute_filename(outfilebasepath, start, end, tablename), "w", 9)
    ctids = ['"(%s,%s)"' % (pg, line) for pg in range(start, end) for line in range(max_linepointers_per_page)]
    copy_range(ctids)
    #is this flush and close necessary?
    fd.flush()
    fd.close()
    log.warning("Finish processing pages in batch from %i to %i", start, end)

log.warning("Done %s. %s", tablename, stats)

conn.close()
