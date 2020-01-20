import os
import shutil
import sys
import tempfile
import functools
import argparse

from typing import Optional, NamedTuple

import xattr  # type: ignore
import humanize  # type: ignore


def memoize(obj):
    """Decorator to memoize a function."""
    cache = obj.cache = {}

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]

    return memoizer


class CephLayout(
    NamedTuple("CephLayout", [("stripe_count", int), ("object_size", int), ("pool", str)])
):
    def __eq__(self, other):
        return (
            self.stripe_count == other.stripe_count
            and self.object_size == other.object_size
            and self.pool == other.pool
        )


class LayoutFixer(object):
    def __init__(self):
        self.LayoutDirs = {}

    def fix_file_layout(self, file):
        pass


@memoize
def extract_layout(filename: str) -> Optional[CephLayout]:
    """Figure out what the file layout for a given directory should be, looking at parent
       directories if necessary."""
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
        attr_tuple = attr.split("=")
        cephlayout[attr_tuple[0]] = attr_tuple[1]
    del cephlayout["stripe_unit"]
    return CephLayout(**cephlayout)


# make a temp dir with the same layout as the given dir
@memoize
def mkdtemp_layout(layout: CephLayout, prefix: str) -> str:
    """Create temporary directory with the given layout"""
    tempdir = tempfile.mkdtemp(dir=prefix)
    xattrs = xattr.xattr(tempdir)
    for attr in layout._fields:
        xattrs.set("ceph.dir.layout.{}".format(attr), bytes(getattr(layout, attr), "utf-8"))
    return tempdir


def relayout_file(filename, tmploc):
    print("copying {} to temp location {}".format(filename, tmploc))
    shutil.copy2(filename, tmploc)
    print("moving back on top of original")
    shutil.move(tmploc, filename)


def main():
    """entrypoint of script"""
    parser = argparse.ArgumentParser(
        description="Ensure cephfs files match their directory layouts"
    )
    parser.add_argument("dir", help="directory to scan")
    parser.add_argument("--tmpdir", default="/c/tmp", help="temporary directory to copy files to")
    args = parser.parse_args()

    total_savings = 0
    total_moved = 0

    session_tmpdir = tempfile.mkdtemp(dir=args.tmpdir)

    print("starting scan of {}".format(args.dir), file=sys.stderr)
    for root, _, files in os.walk(args.dir, topdown=False):
        dir_layout = extract_layout(root)
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
            if dir_layout != file_layout:
                print("file layout doesn't match dir layout: {}".format(file_layout))
                statinfo = os.stat(filename)
                tmploc = os.path.join(tmp_layout_dir, name)
                relayout_file(filename, tmploc)
                oldusage = (statinfo.st_size / 4) * 6
                newusage = (statinfo.st_size / 5) * 7
                savings = oldusage - newusage
                total_moved += 1
                total_savings += savings

    print("saved space in total: {}".format(humanize.naturalsize(total_savings)))
    shutil.rmtree(session_tmpdir)
