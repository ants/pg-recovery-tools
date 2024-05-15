import collections
import heapq
import json
import sys
import re

N = 10

#def format_lsn(lsn):
#    h = f"{lsn:09X}"
#    return h[:-8]+"/"+h[-8:]

class Stats:
    def __init__(self, xid, first_lsn):
        self.xid = xid
        self.first_lsn = first_lsn
        self.commit_time = None
        self.commit_lsn = None
        self.num_updates = 0
    
    def __str__(self):
        return json.dumps({
            "xid": self.xid,
            "lsn_start": self.first_lsn,
            "lsn_end": self.commit_lsn,
            "commit_time": self.commit_time,
            "updates": self.num_updates,
        })

    def __lt__(self, other):
        return self.num_updates < other.num_updates and self.first_lsn < other.first_lsn

    def merge(self, other):
        self.first_lsn = min(self.first_lsn, other.first_lsn)
        self.num_updates += other.num_updates

def print_stats(lsn, topN):
    print(f"=== {lsn} ===")
    for n, s in sorted(topN, reverse=True):
        print(f"{n:8d}: {s}")

def print_running(running, n=3):
    print(f"  Top {n} running:")
    for s in sorted(running.values(), key=lambda s: s.num_updates, reverse=True)[:3]:
        print(f"{s}")

running = collections.defaultdict(int)
topN = [(0,None)]*N

"""
rmgr: Heap        len (rec/tot):     72/    72, tx:     461970, lsn: 0/233C8568, prev 0/233C8520, desc: HOT_UPDATE off 159 xmax 461970 flags 0x20 ; new off 161 xmax 0, blkref #0: rel 1663/13993/16397 blk 5
rmgr: Heap        len (rec/tot):     79/    79, tx:     461971, lsn: 0/233C85B0, prev 0/233C8568, desc: INSERT off 55 flags 0x00, blkref #0: rel 1663/13993/16406 blk 2939
rmgr: Heap        len (rec/tot):     79/    79, tx:     461970, lsn: 0/233C8600, prev 0/233C85B0, desc: INSERT off 98 flags 0x00, blkref #0: rel 1663/13993/16406 blk 2938
rmgr: Transaction len (rec/tot):     34/    34, tx:     461971, lsn: 0/233C8650, prev 0/233C8600, desc: COMMIT 2024-05-15 11:14:34.639229 EEST
"""
parse_re = re.compile(r"rmgr: ([A-Za-z0-9]+) *len \(rec/tot\):\s*\d+/\s*\d+, tx:\s*(\d+), lsn: ([0-9A-F]+/[0-9A-F]+), prev [0-9A-F]+/[0-9A-F]+, desc: ([A-Z_+]+)? (.*)")
"""
('Heap',
 '461970',
 '0/233C8568',
 'HOT_UPDATE',
 'off 159 xmax 461970 flags 0x20 ; new off 161 xmax 0, blkref #0: rel 1663/13993/16397 blk 5')
"""


MOD_CMDS = ('UPDATE', 'HOT_UPDATE', 'INSERT', 'DELETE', 'INSERT+INIT')

last_lsn = None

for i, line in enumerate(sys.stdin):
    match = parse_re.match(line)
    if not match:
        print(line)
        sys.exit(1)
        continue
    rmgr, xid, lsn, cmd, rest = match.groups()
    if rmgr == 'Heap' and cmd in MOD_CMDS:
        stat = running.get(xid)
        if stat is None:
            stat = Stats(xid, lsn)
            running[xid] = stat
        stat.num_updates += 1
    if rmgr == 'Transaction' and cmd == 'COMMIT':
        restparts = rest.split('; ')
        xids = []
        for part in restparts:
            if part.startswith('subxacts: '):
                xids.extend(part[len('subxacts: '):].split(' '))
        stat = running.pop(xid, None)
        for xid in xids:
            if xid not in running:
                continue
            other = running.pop(xid)
            if stat is None:
                stat = other
            else:
                stat.merge(other)
        if stat is not None and stat.num_updates > topN[0][0]:
            stat.commit_lsn = lsn
            stat.commit_time = rest.split(';', 1)[0]
            heapq.heappushpop(topN, (stat.num_updates, stat))
        last_lsn = lsn
    if (i % 1000000) == 999999:
        print_stats(lsn, topN)
        print_running(running)

print_stats(last_lsn, topN)
