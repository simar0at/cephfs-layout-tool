import argparse
import functools
import logging
import os
import shutil
import sys
import tempfile
import subprocess
from typing import NamedTuple, Optional

import humanize  # type: ignore
import xattr  # type: ignore


def memoize(obj):
    """Decorator to memoize a function."""
    # blatantly ripped off of the first google result
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
    logging.info("copying {} to temp location {}".format(filename, tmploc))
    shutil.copy2(filename, tmploc)
    logging.info("moving back on top of original")
    # shutil.move(tmploc, filename) or any python file copy function does not work here (anymore).
    # The data would end up on the same pool it came from.
    # Is this a side effect of the sendfile zero copy trick?
    subprocess.run(['mv', tmploc, filename])

def main():
    """entrypoint of script"""
    parser = argparse.ArgumentParser(
        description="Ensure cephfs files match their directory layouts"
    )
    parser.add_argument("dir", help="directory to scan")
    parser.add_argument("--tmpdir", default="/c/tmp", help="temporary directory to copy files to")
    parser.add_argument("--debug", "-d", action="store_true")
    args = parser.parse_args()

    if args.debug:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    logging.basicConfig(stream=sys.stdout, level=loglevel)

    total_moved = 0

    session_tmpdir = tempfile.mkdtemp(dir=args.tmpdir)

    logging.info("starting scan of {}".format(args.dir))
    for root, _, files in os.walk(args.dir, topdown=False):
        dir_layout = extract_layout(root)
        tmp_layout_dir = mkdtemp_layout(dir_layout, prefix=session_tmpdir)
        for name in files:
            filename = os.path.join(root, name)

            file_layout = extract_layout(filename)
            if not file_layout:
                continue
            if dir_layout != file_layout:
                fstat = os.stat(filename)
                if fstat.st_nlink > 1:
                    logging.debug("skipping {} due to multiple hard links".format(name))
                    continue
                logging.info("file layout doesn't match dir layout: {}".format(file_layout))
                tmploc = os.path.join(tmp_layout_dir, name)
                relayout_file(filename, tmploc)
                total_moved += 1

    shutil.rmtree(session_tmpdir)
