#!/usr/bin/env python3
from __future__ import print_function

import os
import shutil
import sys

import xattr
import humanize

TMPDIR = "/c/scratch/convert"

OK_POOLS = {
    "cephfs_crs52data",
    "cephfs_crs52data2",
    "cephfs_crs52data3",
    "cephfs_crs52data4",
}


def main():
    startdir = sys.argv[1]

    total_savings = 0
    total_moved = 0

    print("starting scan of {}".format(startdir), file=sys.stderr)
    for root, dirs, files in os.walk(startdir, topdown=False):
        print("looking at {}".format(root), file=sys.stderr)
        print(
            "## total savings so far: {} ##".format(humanize.naturalsize(total_savings))
        )
        if root.startswith("/c/archive"):
            continue
        for name in files:
            filename = os.path.join(root, name)
            fstat = os.stat(filename)
            if fstat.st_nlink > 1:
                print("skipping {} due to multiple hard links".format(name))
                continue
            cephlayout = {}
            try:
                for attr in (
                    str(xattr.getxattr(filename, "ceph.file.layout")).strip("'").split()
                ):
                    # print(attr)
                    n = str(attr).split("=")
                    cephlayout[str(n[0])] = str(n[1])
            except IOError as e:
                pass
            # print(cephlayout)
            if "pool" in cephlayout.keys() and cephlayout["pool"] not in OK_POOLS:
                print("%s in wrong pool: %s" % (name, cephlayout["pool"]))
                statinfo = os.stat(filename)
                tmploc = os.path.join(TMPDIR, name)
                print("copying {} to new location and pool {}".format(filename, tmploc))
                shutil.copy2(filename, tmploc)
                print("moving back on top of original")
                shutil.move(tmploc, filename)
                oldusage = (statinfo.st_size / 4) * 6
                newusage = (statinfo.st_size / 5) * 7
                savings = oldusage - newusage
                total_moved += 1
                total_savings += savings
                print("saved {}".format(humanize.naturalsize(savings)))
            # else:
            #    pass

    print("saved space in total: {}".format(humanize.naturalsize(total_savings)))


if __name__ == "__main__":
    main()
