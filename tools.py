#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright notice
# ----------------
#
# Copyright (C) 2013 Daniel Jung
# Contact: d.jung@jacobs-university.de
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Free
# Software Foundation; either version 2 of the License, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
# more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
#
"""Define tools that use the capabilities of the module "h5obj", hence
recognizing Python objects that have been stored inside HDF5 files using
serialization."""
__created__ = '2013-07-08'
__modified__ = '2014-03-21'
import dummy
import fnmatch
import glob
import os
import sys
import h5obj
from columnize import columnize
import numpy
import cofunc

try:
    from frog import Frog
except ImportError:
    Frog = dummy.Decorator


# To do:
# --> add --parents to h5mkgrp and h5rmgrp
# --> is h5ls, don't list groups!! list content of groups
#     not like "find", but like "ls" (without -d option)!
# --> do not have to use "," as a separator in "h5cp", improve frog


@Frog(shortopts=dict(dtype='t', dlen='l', dmax='m', dmin='n'),
      optdoc=dict(dtype='return datatype of the object',
                  dlen='return length of the object',
                  x='return argument "x" of the object',
                  y='return argument "y" of the object',
                  item='get item of the object (integer)',
                  call='call the object with the given argument (float)',
                  dmax='return maximum of the object',
                  dmin='return minimum of the object',
                  attrs='return argument "attrs" of the object',
                  var='return attribute "var" as a coFunc object',
                  stderr='return "sqrt(var/count)" as a coFunc object, ' +
                         'where "var" and "count" are attributes'),
      opttypes=dict(item=int, call=float))
def h5load(fdpath, dtype=False, dlen=False, x=False, y=False, attrs=False,
           item=None, call=None, dmax=False, dmin=False, var=False,
           stderr=False, unpickle=True):
    """Load a dataset from a HDF5 file."""
    # 2013-07-08 - 2014-03-21
    filename, dsetname = h5split(fdpath)
    if not os.path.isfile(filename):
        print >>sys.stderr, 'h5load: cannot load "%s": ' % fdpath +\
                            'no such file or directory'
        sys.exit(1)
    with h5obj.File(filename, 'r', unpickle=unpickle) as f:
        found = dsetname in f
        if found:
            data = f[dsetname]
    if not found:
        print >>sys.stderr, 'h5load: cannot load "%s": no such dataset' \
                            % fdpath
        sys.exit(1)
    if dtype:
        data = type(data)
    if x:
        data = data.x
    if y:
        data = data.y
    if attrs:
        data = data.attrs
    if var:
        data = data.a2cf('var')
    elif stderr:
        var = data.attrs.var
        count = data.attrs.count
        stderr = numpy.sqrt(var/count)
        data = cofunc.coFunc(data.x, stderr)
    if item is not None:
        data = data[item]
    if call is not None:
        data = data(call)
    if dmax:
        data = max(data)
    if dmin:
        data = min(data)
    if dlen:
        data = len(data)
    return data


optdoc = dict(force='overwrite existing datasets', data='data to save')


@Frog(optdoc=optdoc, opttypes=dict(data=str), preproc=dict(data=eval))
def h5save(fdpath, data=None, force=False, pickle=True):
    """Save a dataset to a HDF5 file."""
    filename, dsetname = h5split(fdpath)
    if not filename and fdpath.count('/'):
        filename, dsetname = dsetname.split('/', 1)
    if not filename or not dsetname:
        print >>sys.stderr, 'h5save: no dataset name specified'
        sys.exit(1)
    if os.path.isfile(filename):
        with h5obj.File(filename, 'r') as f:
            found = dsetname in f
    else:
        found = False
    if found and not force:
        print >>sys.stderr, 'h5save: cannot save "%s": dataset exists' \
            % fdpath
        sys.exit(1)
    with h5obj.File(filename, 'a', pickle=pickle) as f:
        if found:
            del f[dsetname]
        f[dsetname] = data


@Frog(postproc=columnize)
def h5ls(fdpath):  # long=False (list objtype, datatype and maybe data)
    """List contents of HDF5 files (and groups). Expect combined
    filename/dataset path. Return list of dataset/group names."""
    filename, dsetname = h5split(fdpath)
    if not os.path.exists(filename):
        _error_fdpath_not_found('h5ls', fdpath)
    if not os.path.isfile(filename):
        _error_not_file('h5ls', filename)
    with h5obj.File(filename, 'r') as f:
        if not dsetname:
            contents = f.keys()
            found = True
        else:
            found = dsetname in f
            if found:
                obj = f[dsetname]
                contents = obj.keys() if type(obj) is h5obj.Group \
                    else [os.path.basename(dsetname)]
    if not found:
        _error_fdpath_not_found('h5ls', fdpath)
    return contents


# do a better h5ls instead? at least there should be only one frog for both
# cases
@Frog(outmap=dict(ALL='#@'))
def h5ll(fdpath):
    """List contents of HDF5 files (and groups), along with some information.
    Expect combined filename/dataset path. Return dict of dict."""
    raise NotImplementedError
    filename, dsetname = h5split(fdpath)
    if not os.path.exists(filename):
        _error_fdpath_not_found('h5ls', fdpath)
    if not os.path.isfile(filename):
        _error_not_file('h5ls', filename)
    with h5obj.File(filename, 'r') as f:
        if not dsetname:
            contents = f.keys()
            found = True
        else:
            found = dsetname in f
            if found:
                obj = f[dsetname]
                contents = obj.keys() if type(obj) is h5obj.Group \
                    else [os.path.basename(dsetname)]
    if not found:
        _error_fdpath_not_found('h5ls', fdpath)
    return contents


optdoc = dict(force='never prompt',
              recursive='remove groups and their contents recursively')


@Frog(optdoc=optdoc)
def h5rm(fdpattern, force=False, recursive=False):
    """Remove datasets from HDF5 files."""
    fdpaths = h5glob(fdpattern)
    if not fdpaths:
        print >>sys.stderr, \
            'h5rm: cannot remove "%s": no such group or dataset' \
            % fdpattern
        sys.exit(1)
    for fdpath in fdpaths:
        filename, dsetname = h5split(fdpath)
        with h5obj.File(filename, 'r') as f:
            dsettype = type(f[dsetname])
        if dsettype is h5obj.Group:
            if not recursive:
                print >>sys.stderr, 'h5rm: cannot remove "%s": is a group' \
                    % fdpath
                sys.exit(1)
        if not force:
            typename = 'group' if dsettype is h5obj.Group else 'dataset'
            message = 'h5rm: remove %s "%s"? ' % (typename, fdpath)
            answer = raw_input(message).lower()
            if not answer or not 'yes'.startswith(answer):
                continue
        with h5obj.File(filename, 'r+') as f:
            del f[dsetname]


@Frog()
def h5mkgrp(fdpath):  # parents=False
    """Create groups in HDF5 files."""
    filename, grpname = h5split(fdpath)
    if not grpname:
        print >>sys.stderr, 'h5mkgrp: no groupname given'
        sys.exit(1)
    if not os.path.isfile(filename):
        print >>sys.stderr, \
            'h5mkgrp: cannot open "%s": no such file' % filename
        sys.exit(1)
    with h5obj.File(filename, 'r+') as f:
        f.create_group(grpname)


@Frog()
def h5rmgrp(fdpattern, ignore_fail_on_non_empty=False):  # parents=False
    """Remove empty groups from HDF5 files."""
    fdpaths = h5glob(fdpattern)
    if not fdpaths:
        print >>sys.stderr, 'h5rmgrp: failed to remove "%s": ' % fdpattern +\
                            'no such group or dataset'
        sys.exit(1)
    for fdpath in fdpaths:
        filename, grpname = h5split(fdpath)
        with h5obj.File(filename, 'r') as f:
            objtype = type(f[grpname])
        if objtype is not h5obj.Group:
            print >>sys.stderr, 'h5rmgrp: cannot remove "%s": is a dataset' \
                                % fdpath
            sys.exit(1)
        with h5obj.File(filename, 'r') as f:
            contents = f[grpname].keys()
        if contents:
            if ignore_fail_on_non_empty:
                continue
            print >>sys.stderr, \
                'h5rmgrp: failed to remove "%s": group not empty' % fdpath
            sys.exit(1)
        with h5obj.File(filename, 'r+') as f:
            del f[grpname]


@Frog()
def h5setitem(fdpattern, key, value):
    """Set an item of a HDF5 dataset to some value."""
    # 2014-06-24
    fdpaths = h5glob(fdpattern)
    if not fdpaths:
        print >>sys.stderr, 'h5setitem: no such dataset: "%s"' % fdpattern
        sys.exit(1)
    for fdpath in fdpaths:
        filename, dsetname = h5split(fdpath)
        with h5obj.File(filename, 'r') as f:
            obj = f[dsetname]
        obj[key] = eval(value)
        h5rm(fdpath, force=True)
        with h5obj.File(filename, 'a') as f:
            f[dsetname] = obj


optdoc = dict(force='overwrite existing datasets',
              recursive='copy groups recursively')


@Frog(optdoc=optdoc)
def h5cp(source, dest, force=False, recursive=False):
    """Copy datasets and groups in HDF5 files (or from one HDF5 file to
    another)."""
    filename, objname = h5split(source)
    with h5obj.File(filename, 'r') as f:
        found = objname in f
        if found:
            objtype = type(f[objname])
    if not found:
        _error_fdpath_not_found(source)
    destfilename, destobjname = h5split(dest)
    if objtype is h5obj.Group:
        if not recursive:
            print >>sys.stderr, 'h5cp: omitting group "%s"' % source
            sys.exit(1)

        # copy group recursively
        #raise NotImplementedError, 'recursive copy not yet implemented'
        ### use h5py.File.copy somehow, could be easier
        dsets = {}
        with h5obj.File(filename, 'r') as f:
            g = f[objname]
            names = []
            g.visit(names.append)
            for name in names:
                if type(g[name]) is not h5obj.Group:
                    dsets[name] = g[name]
        with h5obj.File(destfilename, 'a') as f:
            for name, value in dsets.iteritems():
                f[destobjname+'/'+objname+'/'+name] = value

    else:
        data = h5load(source)
        if os.path.exists(destfilename):
            with h5obj.File(destfilename, 'r') as f:
                found = destobjname in f
                if found:
                    desttype = type(f[destobjname])
        else:
            found = False
        original_name = objname.split('/')[-1]
        if found:
            if desttype is h5obj.Group:
                # save under the original name into that group
                h5save(dest+'/'+original_name, data=data, force=force)
            else:
                # overwrite that dataset (if force)
                h5save(dest, data=data, force=force)
        else:
            # save new dataset under the given name (only if group path
            # exists?)
            new = dest if dest.count('/') else dest+'/'+original_name
            h5save(new, data=data, force=force)


@Frog()
def h5mv(source, dest, recursive=False):
    """Move (rename) datasets in HDF5 files (or from one HDF5 file to
    another)."""
    h5cp(source, dest, recursive=recursive)
    h5rm(source, force=True, recursive=recursive)


def h5glob_filewise(fdpattern, unique=False, sort=False):
    """Expand a combined filename/dataset pattern. Return list of new combined
    filename/dataset patterns, on per file (only dataset part remains a
    pattern)."""
    # 2013-07-12 - 2013-07-12
    filepattern, dsetpattern = h5split(fdpattern)
    new_fdpatterns = []
    filenames = glob.glob(filepattern)
    filenames.sort()
    for filename in filenames:
            new_fdpatterns.append(filename+'/'+dsetpattern)
    return new_fdpatterns

    #dpatterns = {}
    #for fdpath in h5glob(fdpattern):
        #filename, dsetname = h5split(fdpath)
        #if not filename in dpatterns: dpatterns[filename] = []
        #dpatterns[filename].append(dsetname)


def h5glob(fdpattern, unique=False, sort=False):
    """Expand a combined filename/dataset pattern. Return list of single
    combined filename/dataset paths."""
    # 2013-07-08 - 2013-07-08
    filepattern, dsetpattern = h5split(fdpattern)
    if not filepattern:
        return []
    #if not dsetpattern: dsetpattern = '*'
    alldsets = []
    for filename in glob.glob(filepattern):
        if not os.path.isfile(filename):
            raise IOError('"%s" is not a file' % filename)
        allfiledsets = []
        with h5obj.File(filename, 'r') as f:
            f.visit(allfiledsets.append)
        alldsets += ['%s/%s' % (filename, dsetname)
                     for dsetname in fnmatch.filter(allfiledsets, dsetpattern)]
    if unique:
        alldsets = list(set(alldsets))
    if sort:
        alldsets.sort()
    return alldsets


def h5split(pattern):
    """Split a combined filename/dataset pattern into the filename part and the
    dataset part. Return filename pattern and dataset pattern. The given
    pattern must contain at least one slash that devides the filename part from
    the dataset part."""
    # 2013-07-08 - 2013-07-12
    parts = pattern.split('/')
    #if len(parts) < 2:
        #raise ValueError, 'pattern must contain at least one slash "/"'
    for mark in xrange(len(parts)):
        if mark == 0 and not parts[mark]:
            continue
        filepattern = '/'.join(parts[:(mark+1)])
        filenames = glob.glob(filepattern)
        if len(filenames) == 0:  # if actual_files > 0
            filepattern = '/'.join(parts[:mark])
            dsetpattern = '/'.join(parts[mark:])
            break
    else:
        filepattern = '/'.join(parts[:(mark+1)])
        dsetpattern = '/'.join(parts[(mark+1):])
    if dsetpattern and not filepattern:
        filepattern, dsetpattern = dsetpattern, filepattern
    return filepattern, dsetpattern


@Frog(outmap={0: '#@'})
def h5complete(incompl):
    # check if incompl is a pure file path
    globbed = glob.glob(incompl+'*')
    if globbed:
        return [path+'/' for path in globbed]

    # there must be at least one slash
    scount = incompl.count('/')
    if scount == 0:
        return []

    # cycle slashes from back to front, check if the part before the slash is a
    # file
    for sind in xrange(scount, 0, -1):
        part1, part2 = divide(incompl, '/', sind)
        if os.path.isdir(part1):
            break
        if not os.path.isfile(part1):
            continue

        # so part1 is a file
        part2a, part2b = divide(part2, '/', -1)
        try:
            with h5obj.File(part1, 'r') as f:
                g = f[part2a] if part2a else f
                if not type(g) in (h5obj.Group, h5obj.File):
                    return []
                grpitems = g.keys()
                isgrp = {}
                for grpitem in grpitems:
                    isgrp[grpitem] = type(g[grpitem]) is h5obj.Group
        except IOError:
            return []
        filtered = fnmatch.filter(grpitems, part2b+'*')
        if part2a:
            part2a += '/'
        out = []
        for item in filtered:
            name = part1+'/'+part2a+item
            if isgrp[item]:
                name += '/'
            out.append(name)
        return out

    return []


def divide(string, char, n):
    """Divide the string at the n-th occurence of char. Return two strings,
    neither including that char.

    A negative n counts from the back (like normal Python indexing).

    Example:
    >>> divide('a/b/c/d', '/', 1)
    ('a', 'b/c/d')
    """
    parts = string.split(char)
    part1, part2 = parts[:n], parts[n:]
    return char.join(part1), char.join(part2)


def _error_fdpath_not_found(prog, fdpath):
    print >>sys.stderr, '%s: cannot access "%s": no such group or dataset' \
                        % (prog, fdpath)
    sys.exit(1)


def _error_not_file(prog, dirname):
    print >>sys.stderr, '%s: %s: is a directory' % (prog, dirname)
    sys.exit(1)
