from cStringIO import StringIO
import psycopg2
import sys
import time

default_page_range = 100
max_linepointers_per_page = 300


if len(sys.argv) < 4:
    print "Usage trycopy.py connstr srctable outfile"
    sys.exit(1)


connstring = sys.argv[1]
tablename = sys.argv[2]
outfile = sys.argv[3]

fd = open(outfile, "a")

def new_connection():
    global conn, cur
    while True:
        try:
            conn = psycopg2.connect(sys.argv[1])
            break
        except psycopg2.Error:
            time.sleep(1)

    cur = conn.cursor()


new_connection()
cur.execute("SELECT pg_relation_size(%s::regclass)/8192 AS num_pages", (tablename,))
total_pages, = cur.fetchone()




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
            return
        except psycopg2.Error, e:
            if isinstance(e, psycopg2.InterfaceError):
                new_connection()
            else:
                try:
                    conn.rollback()
                except psycopg2.Error, x:
                    new_connection()
            if len(ctids) > 1:
                batch_size = max(1,len(ctids)/10)
                print "Error: %s, bisecting into batches of %d" % (e, batch_size)
                for start in xrange(0,len(ctids),batch_size):
                    copy_range(ctids[start:start+batch_size])
                return
            else:
                print "Failed row %s" % ctids[0]
                return
        finally:
            buf.close()

print "Total pages: %d" % total_pages
last_complete = 0.
for start in xrange(0, total_pages, default_page_range):
    if float(start)/total_pages - last_complete > 0.01:
        last_complete = float(start)/total_pages
        print "%2f%% complete" % (last_complete*100)
    end = min(total_pages, start+default_page_range)
    ctids = ['"(%s,%s)"' % (pg,line) for pg in xrange(start, end) for line in xrange(max_linepointers_per_page)]
    copy_range(ctids)

conn.close()