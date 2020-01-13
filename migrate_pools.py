#!/usr/bin/env python3
import os
import shutil
import sys
import tempfile
from collections import namedtuple
from typing import Optional, Callable

import xattr  # type: ignore
import humanize  # type: ignore

CephLayout = namedtuple("CephLayout", ["stripe_count", "object_size", "pool"])

TMPDIR = "/c/tmp"

OK_POOLS = {"cephfs_crs52data", "cephfs_crs52data2"}


def memoize(fn: Callable):
    """ Memoization decorator for a function taking a single argument """

    class MemoDict(dict):
        def __missing__(self, key):
            ret = self[key] = fn(key)
            return ret

    return MemoDict().__getitem__


@memoize
def extract_layout(filename: str) -> Optional[CephLayout]:
    filetype = "file"
    if os.path.isdir(filename):
        filetype = "dir"
    cephlayout = {}
    try:
        xattrs = (
            xattr.getxattr(filename, "ceph.{}.layout".format(filetype))
            .decode("utf-8")
            .strip("'")
            .split()
        )
    except OSError:
        # no layout on given file/dir
        if filetype == "dir":
            return extract_layout(os.path.dirname(filename))
        return None
    for attr in xattrs:
        n = attr.split("=")
        cephlayout[n[0]] = n[1]
    del cephlayout["stripe_unit"]
    return CephLayout(**cephlayout)


# make a temp dir with the same layout as the given dir
@memoize
def mkdtemp_layout(layout: CephLayout, prefix: str = TMPDIR) -> str:
    tempdir = tempfile.mkdtemp(dir=prefix)
    xattrs = xattr.xattr(tempdir)
    for attr in layout._fields:
        xattrs.set("ceph.dir.layout.{}".format(attr), bytes(getattr(layout, attr), "utf-8"))
    return tempdir


def main():
    startdir = sys.argv[1]

    total_savings = 0
    total_moved = 0

    session_tmpdir = tempfile.mkdtemp(dir=TMPDIR)

    print("starting scan of {}".format(startdir), file=sys.stderr)
    for root, _, files in os.walk(startdir, topdown=False):
        print("looking at {}".format(root), file=sys.stderr)
        print("## total savings so far: {} ##".format(humanize.naturalsize(total_savings)))
        dir_layout = extract_layout(root)
        print("layout for {}: {}".format(root, dir_layout))
        tmp_layout_dir = mkdtemp_layout(dir_layout, prefix=session_tmpdir)
        for name in files:
            filename = os.path.join(root, name)
            fstat = os.stat(filename)
            if fstat.st_nlink > 1:
                print("skipping {} due to multiple hard links".format(name))
                continue
            file_layout = extract_layout(filename)
            if not file_layout:
                continue
            if dir_layout.pool != file_layout.pool:
                print("%s in wrong pool: %s" % (name, file_layout.pool))
                statinfo = os.stat(filename)
                tmploc = os.path.join(tmp_layout_dir, name)
                print("copying {} to temp location {}".format(filename, tmploc))
                shutil.copy2(filename, tmploc)
                print("moving back on top of original")
                shutil.move(tmploc, filename)
                oldusage = (statinfo.st_size / 4) * 6
                newusage = (statinfo.st_size / 5) * 7
                savings = oldusage - newusage
                total_moved += 1
                total_savings += savings
                print("saved {}".format(humanize.naturalsize(savings)))

    print("saved space in total: {}".format(humanize.naturalsize(total_savings)))
    os.rmdir(session_tmpdir)


if __name__ == "__main__":
    main()
