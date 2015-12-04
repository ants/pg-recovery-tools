#!/usr/bin/python
from collections import namedtuple
import logging
from optparse import OptionParser
import os
import struct
import sys

BLOCK = 8192

Page = namedtuple('Page', ['lsn', 'checksum', 'flags', 'pd_lower', 'pd_upper', 
    'pd_special', 'pd_pagesize_version', 'pd_prune_xid'])

log = logging.getLogger('shiftcorruption')
root = logging.getLogger()
fmt = logging.Formatter('[%(asctime)-15s] %(message)s')
fh = logging.FileHandler("shiftcorruption.log")
fh.setFormatter(fmt)
root.addHandler(fh)
sh = logging.StreamHandler()
sh.setFormatter(fmt)
root.addHandler(sh)
root.setLevel("INFO")

def parse_page(data):
    lsn_a, lsn_b, checksum, flags, pd_lower, pd_upper, \
    pd_special, pd_pagesize_version, pd_prune_xid = \
                struct.unpack("IIHHHHHHI", data[0:24])
    return Page((lsn_a << 32) + lsn_b, checksum, flags, pd_lower, pd_upper,
        pd_special, pd_pagesize_version, pd_prune_xid)

def blocks(path, start=0, end=1000000):
    with open(path) as fd:
        index = start
        fd.seek(index*BLOCK)
        buf = fd.read(BLOCK)
        while len(buf) != 0 and index < end:
            yield index, buf
            index += 1
            buf = fd.read(BLOCK)

def blocks_with_prev(path, start=0, end=1000000):
    prev_data = None
    for index, data in blocks(path, start, end):
        yield index, prev_data, data
        prev_data = data

ZERO_BLOCK= "\x00"*8192
def is_zero_page(data):
    if len(data) == BLOCK:
        return data == ZERO_BLOCK
    else:
        return len(data) == data.count("\x00")

def read_block(path, index):
    with open(path) as fd:
        fd.seek(index*BLOCK)
        return fd.read(BLOCK)

def replace_with_backup(backup, index, page):
    if backup is None:
        return None
    backup_data = read_block(backup, index)
    backup_page = parse_page(backup_data)
    if backup_page.lsn == page.lsn:
        return backup_data
    return None

def outLSN(v):
    return "%x/%08x" % (v>>32,v & 0xFFFFFFFF)

class PageStats(object):
    def __init__(self):
        self.lsns = []
        self.xids = []
    
    def add(self, page):
        self.lsns.append(page.lsn)
        if page.pd_prune_xid != 0:
            self.xids.append(page.pd_prune_xid)
    
    def output(self):
        self.lsns.sort()
        self.xids.sort()
        num_parts = 10
        num = len(self.lsns)
        partitions = [int(round(float(i)/num_parts*(num-1))) for i in xrange(0, num_parts)]
        log.info("LSN deciles: %r", [self.lsns[p] for p in partitions])
        log.info("LSN deciles (hex): %r", [outLSN(self.lsns[p]) for p in partitions])
        num = len(self.xids)
        partitions = [int(round(float(i)/num_parts*(num-1))) for i in xrange(0, num_parts)]
        if num > 11:
            log.info("XID deciles: %r", [self.xids[p] for p in partitions])
            log.info("XID deciles (hex): [%s]", ", ".join(["%08x" % self.xids[p] for p in partitions]))
        else:
            log.info("XIDs: %r", self.xids)
            log.info("XIDs (hex): [%s]", ", ".join("%08X"%xid for xid in self.xids))
        
def fix_page_corruption(input_path, validate_page, backup, output):
    size = os.path.getsize(input_path)
    log.info("Processing %s with %d bytes of data (%d pages)" % (input_path, size, size/BLOCK))
    if size % BLOCK != 0:
        err = "Invalid table file size %d. %d extra bytes" % (size, size % BLOCK)
        return err
    out_fd = None
    
    stats = PageStats()
    
    offset = 0
    fixed = 0
    total = 0
    valid = 0
    zero = 0
    unfixable = 0

    for index, prev_data, data in blocks_with_prev(input_path):
        total += 1
        # Find first broken page
        if is_zero_page(data[offset:]):
            if out_fd is not None:
                out_fd.write(prev_data[offset:] + data[:offset])
            zero += 1
            continue
        page = parse_page(data[offset:])
        err = validate_page(page)
        if err is None:
            stats.add(page)
            if offset == 0:
                valid += 1
            else:
                if out_fd is not None:
                    out_fd.write(prev_data[offset:] + data[:offset])
                fixed += 1
            continue
        
        first_invalid = index
        broken_index = index-1
        broken_page = parse_page(prev_data)
        
        log.info("Found broken page header in %s at %d: %s" % (input_path, first_invalid, err))
        
        replace_data = replace_with_backup(backup, broken_index, broken_page)        
        if replace_data is not None:
            fixed += 1
            log.info("Broken page %d can be restored from backup" % broken_index)
        else:
            unfixable += 1
            
        for new_offset in xrange(offset+1,BLOCK-24):
            if validate_page(parse_page(data[new_offset:])) is None:
                break
        else:
            return "Broken page %d in %s can not be fixed by shifting"
        offset = new_offset
        log.info("Found a fix with offset %d" % offset)
        
        if output:
            if out_fd is None:
                out_fd = open(output, 'w')
                # Copy out blocks until first broken page
                for l, copy_data in blocks(input_path, 0, broken_index):
                    out_fd.write(copy_data)
                log.info("Copied %d pages directly" % l)
            if replace_data is not None:
                stats.add(parse_page(replace_data))
                out_fd.write(replace_data)
            else:
                out_fd.write(ZERO_BLOCK)
    final_index = index
    if offset != 0:
        final_page = parse_page(read_block(input_path, final_index)[offset:])
        replace_data = replace_with_backup(backup, final_index, final_page)
        if replace_data is not None:
            log.info("Final page can be restored from backup")
            stats.add(parse_page(replace_data))
            fixed += 1
            if out_fd is not None:
                out_fd.write(replace_data)
        else:
            unfixable += 1
            if out_fd is not None:
                out_fd.write(ZERO_BLOCK)
    stats.output()
    if offset == 0:
        log.info("File %s is fine" % input_path) 
    else:
        log.info("Found %d pages in %s, %d empty. Fixed %d pages, %d unfixable, %d valid" % (total, input_path, zero, fixed, unfixable, valid))
    return None

def page_validator(lsn_min=3, lsn_max=2**48, xid_min=0, xid_max=2**32, special_min=8192):
    def validate_page(page):
        if not (lsn_min <= page.lsn < lsn_max):
            return "Invalid LSN: %d" % page.lsn
        
        if page.checksum != 0:
            return "Invalid checksum %d" % page.checksum
        
        if page.flags > 0x7 :
            return "Invalid flags %04X" % page.flags
        
        if page.pd_lower > page.pd_upper:
            return "Negative free space between %d and %d" % (page.pd_lower, page.pd_upper)
        
        if page.pd_upper > page.pd_special:
            return "Upper %d above special %d" % (page.pd_upper, page.pd_special)
        
        if page.pd_special > BLOCK or page.pd_special & 0x7 != 0:
            return "Invalid pd_special %d" % page.pd_special
        
        if page.pd_pagesize_version != 0x2004:
            return "Invalid pagesize version %04X" % page.pd_pagesize_version
        
        if not (page.pd_prune_xid == 0 or xid_min <= page.pd_prune_xid < xid_max):
            return "Invalid prune xid %d" % page.pd_prune_xid
        
        return None
    return validate_page
                

if __name__ == '__main__':
    parser = OptionParser(usage="usage: $prog [options] broken_file",
        description="""Small utility to fix corruption where garbage bytes
        have been randomly inserted into relation segments, shifting the rest
        of the file by an offset. Currently only works sanely on heap files.
        
        Parses one file, optionally tries to replace bad pages (page with inserted
        garbage and final cropped page) from backup if page LSNs match. If pages
        can not be fixed they will be replaced with zeroed pages.
        
        Logs are appended to shiftcorruption.log in the local dir.
        
        Header validation can be tightened by LSN and XID bounds, special space is
        disallowed by default.""")
    parser.add_option("-b", "--backup", dest="backup",
                  help="compare with backup FILE", metavar="FILE")
    parser.add_option("-o", "--output", dest="output",
                  help="output fixed FILE", metavar="FILE")
    parser.add_option("--lsnmin", dest="lsnmin", type="int",
                  help="minimum LSN value allowed in valid page headers", metavar="LSN",
                  default=3)
    parser.add_option("--lsnmax", dest="lsnmax", type="int",
                  help="maximum LSN value allowed in valid page headers", metavar="LSN",
                  default=2**48)
    parser.add_option("--xidmin", dest="xidmin", type="int",
                  help="minimum XID value allowed in valid page headers", metavar="LSN",
                  default=0)
    parser.add_option("--xidmax", dest="xidmax", type="int",
                  help="minimum XID value allowed in valid page headers", metavar="LSN",
                  default=2**32)
    parser.add_option("--specialmin", dest="specialmin", type="int",
                  help="minimum special spave", metavar="LSN",
                  default=BLOCK)
    (options, args) = parser.parse_args()
    if len(args) < 1:
        print "Usage: %s filenode" % (sys.argv[0])
        sys.exit(1)
        
    validate_page = page_validator(
        lsn_min=options.lsnmin,
        lsn_max=options.lsnmax,
        xid_min=options.xidmin,
        xid_max=options.xidmax,
        special_min=options.specialmin,
    )
    err = fix_page_corruption(args[0], validate_page, options.backup, options.output)
    if err != None:
        log.error(err)
        sys.exit(2)