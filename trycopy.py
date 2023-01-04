#from cStringIO import StringIO
from io import StringIO
import logging
import psycopg2
import sys
import time
import csv
from optparse import OptionParser

parser = OptionParser(usage="usage: %prog [options] connstr srctable outfile",
    description="""Tries to copy out as much data as possible from a broken table.""")
parser.add_option("-c", "--csv", dest="csv",
                help="store broken ctids into csv FILE", metavar="FILE")
parser.add_option("-l", "--log", dest="log",
                help="store logs into FILE", metavar="FILE")
parser.add_option("-p", "--lpmax", dest="lpmax", type="int", default=300,
                help="maximum number of linepointers in a page")
parser.add_option("-b", "--batch", dest="batch", type="int", default=100,
                help="Starting batch size in pages")

(options, args) = parser.parse_args()


default_page_range = options.batch
max_linepointers_per_page = options.lpmax


if len(args) != 3:
    parser.print_usage()
    sys.exit(1)

connstring = args[0]
tablename = args[1]
outfile = args[2]


root = logging.getLogger()
fmt = logging.Formatter('[%(asctime)-15s] %(message)s')
if options.log:
    fh = logging.FileHandler(options.log)
    fh.setFormatter(fmt)
    root.addHandler(fh)
sh = logging.StreamHandler()
sh.setFormatter(fmt)
sh.setLevel(logging.WARN)
root.addHandler(sh)
root.setLevel(logging.INFO)

log = logging.getLogger('trycopy')
log.setLevel(logging.INFO)


if options.csv:
    csvfd = open(options.csv, "a")
    csvwriter = csv.writer(csvfd)
else:
    csvwriter = None

fd = open(outfile, "w")

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
            fd.write(buf.getvalue())
            stats.success += cur.rowcount
            return
        except psycopg2.Error as e:
            if isinstance(e, psycopg2.InterfaceError):
                new_connection()
            else:
                try:
                    conn.rollback()
                except psycopg2.Error as x:
                    new_connection()
            if len(ctids) > 1:
                batch_size = max(1,len(ctids)//10)
                log.info("Error: %s, bisecting into batches of %d" % (e, batch_size))
                for start in range(0,len(ctids),batch_size):
                    copy_range(ctids[start:start+batch_size])
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
log.warn( "Total pages: %d" % total_pages)
last_complete = 0.
for start in range(0, total_pages, default_page_range):
    if float(start)/total_pages - last_complete > 0.01:
        last_complete = float(start)/total_pages
        log.warn("%2f%% complete, %s", (last_complete*100), stats)
    end = min(total_pages, start+default_page_range)
    ctids = ['"(%s,%s)"' % (pg,line) for pg in range(start, end) for line in range(max_linepointers_per_page)]
    copy_range(ctids)

log.warn("Done %s. %s", tablename, stats)

conn.close()

