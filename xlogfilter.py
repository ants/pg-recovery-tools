#!/usr/bin/python
from prototype import *
import crc32
import sys
import struct
from collections import namedtuple
import os

XLogPageHeader = namedtuple('XLogPageHeader',
                                ['magic', 'info', 'tli', 'pageaddr', 'rem_len'])
XLogLongPageHeader = namedtuple('XLogLongPageHeader',
                                ['magic', 'info', 'tli', 'pageaddr', 'rem_len',
                                 'sysid', 'seg_size', 'xlog_blcksz'])
XLogRecord = namedtuple("XLogRecord", ["tot_len", "xid", "len", "info", "rmid", "prev", "crc"])
RelFileNode = namedtuple("RelFileNode", [
    'spcNode', 'dbNode', 'relNode'
])
BkpBlock = namedtuple("BkpBlock", [
    'node', 'fork', 'block', 'hole_offset', 'hole_length'
])

RECORD_HEADER_LEN = 32

RM_NAMES = [
    "XLOG",
    "Transaction",
    "Storage",
    "CLOG",
    "Database",
    "Tablespace",
    "MultiXact",
    "RelMap",
    "Standby",
    "Heap2",
    "Heap",
    "Btree",
    "Hash",
    "Gin",
    "Gist",
    "Sequence",
    "SPGist",
]
RM_XLOG_ID = 0
RM_XACT_ID = 1
RM_SMGR_ID = 2
RM_CLOG_ID = 3
RM_DBASE_ID = 4
RM_TBLSPC_ID = 5
RM_MULTIXACT_ID = 6
RM_RELMAP_ID = 7
RM_STANDBY_ID = 8
RM_HEAP2_ID = 9
RM_HEAP_ID = 10
RM_BTREE_ID = 11
RM_HASH_ID = 12
RM_GIN_ID = 13
RM_GIST_ID = 14
RM_SEQ_ID = 15
RM_SPGIST_ID = 16



INFOS = {
    RM_XLOG_ID: {
        0x00: "XLOG_CHECKPOINT_SHUTDOWN",
        0x10: "XLOG_CHECKPOINT_ONLINE",
        0x20: "XLOG_NOOP",
        0x30: "XLOG_NEXTOID",
        0x40: "XLOG_SWITCH",
        0x50: "XLOG_BACKUP_END",
        0x60: "XLOG_PARAMETER_CHANGE",
        0x70: "XLOG_RESTORE_POINT",
        0x80: "XLOG_FPW_CHANGE",
        0x90: "XLOG_END_OF_RECOVERY",
        0xA0: "XLOG_FPI",
    },
    RM_XACT_ID: {
        0x00: "XLOG_XACT_COMMIT",
        0x10: "XLOG_XACT_PREPARE",
        0x20: "XLOG_XACT_ABORT",
        0x30: "XLOG_XACT_COMMIT_PREPARED",
        0x40: "XLOG_XACT_ABORT_PREPARED",
        0x50: "XLOG_XACT_ASSIGNMENT",
        0x60: "XLOG_XACT_COMMIT_COMPACT",
    },
    RM_SMGR_ID: {
        0x10: "XLOG_SMGR_CREATE",
        0x20: "XLOG_SMGR_TRUNCATE",        
    },
    RM_CLOG_ID: {
        0x00: "CLOG_ZEROPAGE",
        0x10: "CLOG_TRUNCATE",        
    },
    RM_DBASE_ID: {
        0x10: "XLOG_SMGR_CREATE",
        0x20: "XLOG_SMGR_TRUNCATE",        
    },
    RM_TBLSPC_ID: {
        0x00: "XLOG_TBLSPC_CREATE",
        0x10: "XLOG_TBLSPC_DROP",
    },
    RM_MULTIXACT_ID: {
        0x00: "XLOG_MULTIXACT_ZERO_OFF_PAGE",
        0x10: "XLOG_MULTIXACT_ZERO_MEM_PAGE",
        0x20: "XLOG_MULTIXACT_CREATE_ID",
    },
    RM_RELMAP_ID: {
        0x00: "XLOG_RELMAP_UPDATE",        
    },
    RM_STANDBY_ID: {
        0x00: "XLOG_STANDBY_LOCK",
        0x10: "XLOG_RUNNING_XACTS",
    },
    RM_HEAP2_ID: {
        0x00: "XLOG_HEAP2_REWRITE",
        0x10: "XLOG_HEAP2_CLEAN",
        0x20: "XLOG_HEAP2_FREEZE_PAGE",
        0x30: "XLOG_HEAP2_CLEANUP_INFO",
        0x40: "XLOG_HEAP2_VISIBLE",
        0x50: "XLOG_HEAP2_MULTI_INSERT",
        0x60: "XLOG_HEAP2_LOCK_UPDATED",
        0x70: "XLOG_HEAP2_NEW_CID",
    },
    RM_HEAP_ID: {
        0x00: "XLOG_HEAP_INSERT",
        0x10: "XLOG_HEAP_DELETE",
        0x20: "XLOG_HEAP_UPDATE",
        0x40: "XLOG_HEAP_HOT_UPDATE",
        0x50: "XLOG_HEAP_NEWPAGE",
        0x60: "XLOG_HEAP_LOCK",
        0x70: "XLOG_HEAP_INPLACE",
        0x70: "XLOG_HEAP_OPMASK",
        0x80: "XLOG_HEAP_INIT_PAGE",
    },
    RM_BTREE_ID: {
        0x00: "XLOG_BTREE_INSERT_LEAF",
        0x10: "XLOG_BTREE_INSERT_UPPER",
        0x20: "XLOG_BTREE_INSERT_META",
        0x30: "XLOG_BTREE_SPLIT_L",
        0x40: "XLOG_BTREE_SPLIT_R",
        0x50: "XLOG_BTREE_SPLIT_L_ROOT",
        0x60: "XLOG_BTREE_SPLIT_R_ROOT",
        0x70: "XLOG_BTREE_DELETE",
        0x80: "XLOG_BTREE_UNLINK_PAGE",
        0x90: "XLOG_BTREE_UNLINK_PAGE_META",
        0xA0: "XLOG_BTREE_NEWROOT",
        0xB0: "XLOG_BTREE_MARK_PAGE_HALFDEAD",
        0xC0: "XLOG_BTREE_VACUUM",
        0xD0: "XLOG_BTREE_REUSE_PAGE",
    },
    RM_HASH_ID: {},
    RM_GIN_ID: {
        0x00: "XLOG_GIN_CREATE_INDEX",
        0x10: "XLOG_GIN_CREATE_PTREE",
        0x20: "XLOG_GIN_INSERT",
        0x30: "XLOG_GIN_SPLIT",
        0x40: "XLOG_GIN_VACUUM_PAGE",
        0x50: "XLOG_GIN_DELETE_PAGE",
        0x60: "XLOG_GIN_UPDATE_META_PAGE",
        0x70: "XLOG_GIN_INSERT_LISTPAGE",
        0x80: "XLOG_GIN_DELETE_LISTPAGE",
        0x90: "XLOG_GIN_VACUUM_DATA_LEAF_PAGE",
    },
    RM_GIST_ID: {
        0x00: "XLOG_GIST_PAGE_UPDATE",
        0x20: "XLOG_GIST_NEW_ROOT",
        0x30: "XLOG_GIST_PAGE_SPLIT",
        0x40: "XLOG_GIST_INSERT_COMPLETE",
        0x50: "XLOG_GIST_CREATE_INDEX",
        0x60: "XLOG_GIST_PAGE_DELETE",
    },
    RM_SEQ_ID: {
        0x00: "XLOG_SEQ_LOG",
    },
    RM_SPGIST_ID: {
        0x00: "XLOG_SPGIST_CREATE_INDEX",
        0x10: "XLOG_SPGIST_ADD_LEAF",
        0x20: "XLOG_SPGIST_MOVE_LEAFS",
        0x30: "XLOG_SPGIST_ADD_NODE",
        0x40: "XLOG_SPGIST_SPLIT_TUPLE",
        0x50: "XLOG_SPGIST_PICKSPLIT",
        0x60: "XLOG_SPGIST_VACUUM_LEAF",
        0x70: "XLOG_SPGIST_VACUUM_ROOT",
        0x80: "XLOG_SPGIST_VACUUM_REDIRECT",
    },
}
I = dict((name,i) for rmgr, infos in INFOS.items() for i, name in infos.items())
"""    "Storage",
    "CLOG",
    "Database",
    "Tablespace",
    "MultiXact",
    "RelMap",
    "Standby",
    "Heap2",
    "Heap",
    "Btree",
    "Hash",
    "Gin",
    "Gist",
    "Sequence",
    "SPGist",
}"""
    

def align4(v):
    return (v+0x3)&~0x3

def align8(v):
    return (v+0x7)&~0x7

def parse_relfilenode(data):
    assert len(data)>=12
    return RelFileNode(*struct.unpack("III", data[0:12]))

def parse_record(data):
    assert len(data) >= RECORD_HEADER_LEN
    return XLogRecord(*struct.unpack("IIIBBLI", data[0:28]))

class Record(object):
    def __init__(self, lsn, header, rmdata, blocks):
        self.lsn = lsn
        self.header = header
        self.rmdata = rmdata
        self.blocks = blocks

    @classmethod
    def read_from(cls, fd):
        rmdata = None
        blocks = []
        
        lsn, data = fd.read(RECORD_HEADER_LEN, align=True)
        header = parse_record(data)
        #print "rec %08x: %r" % (lsn,header,)
        if header.tot_len == 0:
            raise StopIteration()
        if header.len != 0:
            _, rmdata = fd.read(header.len)
        
        backupblockslen = header.tot_len - header.len - RECORD_HEADER_LEN
        if backupblockslen:
            _, blockdata = fd.read(backupblockslen)
            offset = 0
            for i in xrange(bin(header.info & 0x0F).count('1')):
                node = parse_relfilenode(blockdata[offset:offset+12])
                block = BkpBlock(node, *struct.unpack("IIHH", blockdata[offset+12:offset+24]))
                content_len = (8192 - block.hole_length)
                contents = blockdata[offset+24:offset+24+content_len]
                offset += 24+content_len
                blocks.append((block, contents))
        
        if header.rmid == RM_XLOG_ID and (header.info & 0xF0 == I["XLOG_SWITCH"]):
            nlsn = fd.pos
            to_end_of_page = XLOG_BLCKSZ - (nlsn & XLOG_BLCK_MASK)
            next_page = (nlsn & ~XLOG_BLCK_MASK) + XLOG_BLCKSZ
            next_seg = (nlsn & ~XLOG_SIZE_MASK) + XLOG_SIZE
            num_blocks = (next_seg - next_page) / XLOG_BLCKSZ
            data_per_block = XLOG_BLCKSZ - HEADER_LEN
            
            #print "%08x %08x" % (lsn, nlsn)
            #print "End of page in %d" % to_end_of_page
            #print "Next page @ %08X" % next_page
            #print "Next seg @ %08X" % next_seg
            #print "Num blocks %d" % num_blocks
            #print "Total: %d" % (to_end_of_page + num_blocks*data_per_block)
            fd.read(to_end_of_page + num_blocks*data_per_block)

        return cls(lsn, header, rmdata, blocks)

    @property
    def num_blocks(self):
        return bin(self.header.info & 0x0F).count('1')

    @property
    def rm_name(self):
        return RM_NAMES[self.header.rmid]

    @property
    def rmid(self):
        return self.header.rmid

    @property
    def info(self):
        return self.header.info & 0xF0

    def __repr__(self):
        rec = self.header
        
        info = rec.info&0xF0
        infoname = INFOS[self.rmid].get(info, info)
        return "%08x %12s.%-27s(xid=%d, tot_len=%d, prev=%09x, data=%d, blocks=[%s])" % (
            self.lsn, self.rm_name, infoname, rec.xid, rec.tot_len, rec.prev,
            rec.len, ", ".join(repr(blk) for blk, contents in self.blocks)
        )

def read_xlog_long_page_header(src):
    data = src.read(40)
    return XLogLongPageHeader(*struct.unpack("HHILILII", data))

def read_xlog_page_header(src):
    data = src.read(24)
    return XLogPageHeader(*struct.unpack("HHILI", data[0:20]))

XLOG_SIZE = 16*1024*1024

XLOG_SIZE_MASK = XLOG_SIZE-1
XLOG_BLCKSZ = 8192
XLOG_BLCK_MASK = XLOG_BLCKSZ-1
LONG_HEADER_LEN = 40
HEADER_LEN = 24

class xlogfilereader(object):
    def __init__(self, path):
        self.path = path
        self.tli = 1
        self.seg = 1
        self.files = self.xlog_files()
        self.cur_file = open(next(self.files))
        self.remaining = XLOG_SIZE
        self.start_lsn = XLOG_SIZE*self.seg
        
    def xlog_files(self):
        seg = self.seg
        while True:
            path = "%s/%08X%08X%08X" % (self.path, self.tli, seg>>8, seg&0xFF) 
            if not os.path.exists(path):
                print path, "does not exist"
                return
            yield path
            seg += 1

    def read(self, amount):
        assert amount > 0
        if amount < self.remaining:
            self.remaining -= amount
            return self.cur_file.read(amount)
        buf = self.cur_file.read(self.remaining)
        amount -= self.remaining
        self.cur_file.close()
        self.cur_file = open(next(self.files))
        self.remaining = XLOG_SIZE
        self.remaining -= amount
        return buf + self.cur_file.read(amount)

class xlogreader(object):
    def __init__(self, filereader):
        self.fd = filereader
        self.pos = filereader.start_lsn
        
    def read(self, amount, align=False):
        def read_header():
            if self.pos % XLOG_SIZE == 0:
                #print
                #print "    ",
                header = read_xlog_long_page_header(self.fd)
                print header
                self.pos += 40
            elif self.pos % 8192 == 0:        
                #print
                #print "    ",
                header = read_xlog_page_header(self.fd)
                self.pos += 24
        
        #print "Reading %d at %04x" % (amount, self.pos)


        if align and self.pos & 7:
            newpos = align8(self.pos)
            self.fd.read(newpos - self.pos)
            #print "  aligned to %04x by %d" % (newpos, newpos - self.pos)
            self.pos = newpos
        
        read_header()

        buf = ""
        lsn = self.pos
        amount_todo = amount
        free = (8192 - (self.pos % 8192))
        
        while amount_todo > free:
            buf += self.fd.read(free)
            self.pos += free
            read_header()
            amount_todo -= free
            free = 8192-24
        buf += self.fd.read(amount_todo)
        self.pos += amount_todo
                
        return lsn, buf

def records(path):
    reader = xlogreader(xlogfilereader(path))
    while True:
        yield Record.read_from(reader)

def show_node(node):
    return "N(%s, %s, %s)" % node

maxalign = align8

def at_page_boundary(lsn):
    return (lsn & XLOG_BLCK_MASK) == 0

def at_segment_boundary(lsn):
    return (lsn & XLOG_SIZE_MASK) == 0

def next_page(lsn):
    return lsn & ~XLOG_BLCK_MASK + XLOG_BLCKSZ

def next_seg(lsn):
    return lsn & ~XLOG_SIZE_MASK + XLOG_SIZE

CHUNK_HEADER = 0
CHUNK_DATA = 1

FILENODE_LEN = 12

def iterate_chunks(lsn, data):
    offset = 0
    datalen = len(data)
    while offset < datalen:
        if lsn & XLOG_SIZE_MASK < LONG_HEADER_LEN:
            amount = LONG_HEADER_LEN - (lsn & XLOG_SIZE_MASK)
            chunktype = CHUNK_HEADER
        elif lsn & XLOG_BLCK_MASK < HEADER_LEN:
            amount = HEADER_LEN - (lsn & XLOG_BLCK_MASK)
            chunktype = CHUNK_HEADER
        else:
            amount = XLOG_BLCKSZ - lsn & XLOG_BLCK_MASK
            chunktype = CHUNK_DATA
        
        end = offset+amount
        if end > datalen:
            end = datalen
        yield lsn, chunktype, data[offset:end]
        lsn += end - offset
        offset = end

def write_noop_rec(buf, rec):
    "tot_len", "xid", "len", "info", "rmid", "prev", "crc"
    struct.pack_into("IIIBBLI", buf, 0,
                rec.tot_len,
                rec.xid,
                rec.tot_len - RECORD_HEADER_LEN,
                I["XLOG_NOOP"],
                RM_XLOG_ID,
                rec.prev,
                0)
    struct.pack_into("III", buf, RECORD_HEADER_LEN, 0,0,0)
    crc = crc32.pgcrc32_arr(buf[0:24], init_zeroes=rec.tot_len - RECORD_HEADER_LEN)
    struct.pack_into("I", buf, 24, crc)

def filter_machine(start_lsn, src, dest, exclude_filenodes):
    state, substate = "copy", "normal"
    amount = 0
    buf = bytearray(16*8192)
    buf_offset = 0
    buf_lsn = 0
    buf_headers = []
    rem_len = 0
    
    
    def write_out_buf():
        offset = 0
        cur_lsn = buf_lsn
        for head_lsn, header in buf_headers:
            amount = head_lsn - cur_lsn
            assert offset+amount <= buf_offset
            dest.write(buf[offset:offset+amount])
            dest.write(header)
            #print "- From buffer %d B data %d B header" % (amount, len(header))
            offset += amount
            cur_lsn = head_lsn + len(header)
        dest.write(buf[offset:buf_offset])
        #print "- From buffer %d B data" % (buf_offset - offset)

    lsn = start_lsn
    for data in src:
        for cur_lsn, chunktype, chunk in iterate_chunks(lsn, data):
            #print "Got chunk at %10X, type %s, length %d" % (cur_lsn, chunktype, len(chunk))
            offset = 0
            chunklen = len(chunk)
            while offset < chunklen:
                if chunktype is CHUNK_HEADER:
                    if state == "copy":
                        #print "- Copy header %d bytes" % chunklen
                        dest.write(chunk)
                        if substate == "switch" and chunklen == LONG_HEADER_LEN:
                            state, substate = "buffer", "record"
                            amount = RECORD_HEADER_LEN
                            # Reset buffering state
                            buf_offset = 0
                            buf_lsn = cur_lsn + LONG_HEADER_LEN
                            del buf_headers[:]
                    elif state == "buffer":
                        #print "- Buffer header at %10X, %d bytes" % (cur_lsn+offset, chunklen)
                        buf_headers.append((cur_lsn+offset, chunk))
                    offset += chunklen
                elif chunktype is CHUNK_DATA:
                    if offset + amount > chunklen:
                        subchunk = chunk[offset:]
                        inc = chunklen - offset
                    else:
                        subchunk = chunk[offset:offset+amount]
                        inc = amount
                    #print "- Got %d/%d bytes of data" % (inc, amount)
                    amount -= inc
                    offset += inc

                    if state == "copy":
                        if substate == "normal" or substate == "switch":
                            #print "- Copy %d bytes of data" % len(subchunk)
                            dest.write(subchunk)
                        elif substate == "zero":
                            #print "- Zero %d bytes of data" % len(subchunk)
                            dest.write("\x00"*len(subchunk))
                            
                        if not amount and substate != "switch":
                            to_align = maxalign(cur_lsn+offset) - cur_lsn - offset
                            if to_align:
                                #print "- Need to align by %d bytes" % to_align
                                amount = to_align
                            else:
                                #print "- Switch state to read record"
                                state, substate = "buffer", "record"
                                amount = RECORD_HEADER_LEN
                                # Reset buffering state
                                buf_offset = 0
                                buf_lsn = cur_lsn + offset
                                del buf_headers[:]
                    elif state == "buffer":
                        buf[buf_offset:buf_offset+inc] = subchunk
                        buf_offset += inc
                        #print "- Buffered %d bytes of data" % len(subchunk)
                        if not amount:
                            if substate == "record":
                                #print "                              - Got record at %08X" % buf_lsn
                                rec = parse_record(buf[0:RECORD_HEADER_LEN])
                                rem_len = rec.tot_len - RECORD_HEADER_LEN
                                
                                if rec.tot_len == 0:
                                    print "End of WAL"
                                    return
                                elif rec.rmid == RM_XLOG_ID and (rec.info & 0xF0) == I["XLOG_SWITCH"]:
                                    #print "Xlog_switch at %08X" % buf_lsn
                                    write_out_buf()
                                    state, substate = "copy", "switch"
                                    amount = XLOG_SIZE
                                #if rec.rmid == RM_XLOG_ID and rec.info == I["XLOG_FPI"]:
                                elif rec.rmid in [RM_SMGR_ID, RM_HEAP_ID, RM_HEAP2_ID, RM_BTREE_ID,
                                                RM_GIN_ID, RM_GIST_ID, RM_SEQ_ID, RM_SPGIST_ID] or (
                                     rec.rmid == RM_XLOG_ID and (rec.info & 0xF0) == I["XLOG_FPI"]
                                    ):
                                    #print "- Switch state to filenode"
                                    amount = FILENODE_LEN
                                    rem_len -= FILENODE_LEN
                                    substate = "filenode"
                                else:
                                    #print "- Copy out rest %d/%d bytes of the record" % (rem_len, rem_len + RECORD_HEADER_LEN)
                                    write_out_buf()
                                    state, substate = "copy", "normal"
                                    amount = rem_len
                            elif substate == "filenode":
                                node = parse_relfilenode(buf[RECORD_HEADER_LEN:RECORD_HEADER_LEN+FILENODE_LEN])
                                if node in exclude_filenodes:
                                    print "        - Filter record %s at %08X" % (node, buf_lsn)
                                    write_noop_rec(buf, rec)                                    
                                    write_out_buf()
                                    state, substate = "copy", "zero"
                                    amount = rem_len
                                else:
                                    #print "- Passthrough record"
                                    write_out_buf()
                                    state, substate = "copy", "normal"
                                    amount = rem_len
                                
        lsn += len(data)

class WalWriter(object):
    def __init__(self, path, tli, start_lsn):
        self.path = path
        self.tli = tli
        self.start_lsn = start_lsn
        self.lsn = start_lsn
        #print "Start_lsn", start_lsn
        self.seg = start_lsn >> 24
        offset = start_lsn&0xFFFFFF
        path = self.output_path
        self.output = None
    
    @property
    def output_path(self):
        #print "output to %s/%08X%08X%08X" % (self.path, self.tli, self.seg>>8, self.seg&0xFF)
        return "%s/%08X%08X%08X" % (self.path, self.tli, self.seg>>8, self.seg&0xFF)
    
    def write(self, data):
        if self.output is None:
            self.output = open(self.output_path, 'w')

        self.output.write(data)
        self.lsn += len(data)
        if len(data) and self.lsn & XLOG_SIZE_MASK == 0:
            self.output.close()
            self.output = None
            self.seg += 1

def parse_xlog_filename(filename):
    tli = int(filename[0:8], 16)
    seg = (int(filename[8:16], 16)<<8) + int(filename[16:], 16)
    return tli, seg

import sys
def read_files(startfile):
    filename = os.path.basename(startfile)
    path = os.path.dirname(startfile)
    
    tli, seg = parse_xlog_filename(filename)
    
    xlogfile = "%s/%08X%08X%08X" % (path, tli, seg>>8, seg&0xFF)
    while os.path.exists(xlogfile):
        print "    - filtering %r" % xlogfile
        with open(xlogfile) as fd:
            data = fd.read(8192)
            while data != "":
                yield data
                data = fd.read(8192)
        seg += 1
        xlogfile = "%s/%08X%08X%08X" % (path, tli, seg>>8, seg&0xFF)

from optparse import OptionParser

parser = OptionParser()
parser.add_option("-x", "--exclude", dest="exclude", action="append", type="string",
                help="Filter out a filenode. Format: tablespaceoid,databaseoid,filenode")
(options, args) = parser.parse_args()

if len(args) < 2:
    print "Usage: %s [-x 12345,67890,12435] startseg outdir" % sys.argv[0]
    sys.exit(1)

import re

filenode_re = re.compile("^([0-9]+),([0-9]+),([0-9]+)$")

excludes = set()
if options.exclude:
    for exclude in options.exclude:
        match = filenode_re.match(exclude)
        if not match:
            print "Invalid filenode %s" % exclude
            sys.exit(1)
        excludes.add(RelFileNode(*map(int, match.groups())))

start_file = args[0]
outpath = args[1]
tli, seg = parse_xlog_filename(os.path.basename(start_file))
start_lsn = seg*XLOG_SIZE
writer = WalWriter(outpath, tli, start_lsn)

filter_machine(start_lsn, read_files(start_file), writer, excludes)





















