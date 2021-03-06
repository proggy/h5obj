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
"""This module defines a wrapper for the class h5py.File (and h5py.Group), and
adds to it the ability to store any Python object (including nested and empty
lists and tuples, the None type, user-defined objects etc.) using the concept
of serialization. It uses the cPickle module for objects which fail to convert
properly to one of the default HDF5 datatypes, and when loading strings, it
tries to deserialize it before loading the string the normal way. It also makes
sure that you get back exactly the datatype that you were saving to the HDF5
file (unlike h5py.File, where you usually save a tuple or list, and get back a
numpy.ndarray instead).

If in certain situations, the attempt to pickle/unpickle the data is not
wanted, it can be switched off using the attributes "pickle" and "unpickle" of
the classes "Group" and "File".

This module depends on the module "h5py". It is available at
"http://www.h5py.org/" under a BSD license.

To do:
--> make get-method unpickle (so far it just calls the original get-method)
"""
__created__ = '2013-07-07'
__modified__ = '2013-07-18'
__version__ = '0.1'
# former h5obj (developed 2011-10-21 until 2013-03-14)
import collections
import cPickle
import h5py


class Group(collections.MutableMapping):
    """Wrapper for h5py.Group, using serialization for objects that normally
    would not be able to be stored in a HDF5 file."""
    __created__ = '2013-07-07'
    __modified__ = '2013-07-07'

    def __init__(self, h5group, pickle=True, unpickle=True, return_value=True):
        self.h5group = h5group
        self.pickle = pickle
        self.unpickle = unpickle
        self.return_value = return_value

    def create_group(self, name):
        return Group(self.h5group.create_group(name))

    def require_group(self, name):
        return Group(self.h5group.require_group(name))

    def create_dataset(self, name, shape=None, dtype=None, data=None,
                       **kwargs):
        return self.h5group.create_dataset(name, shape=None, dtype=None,
                                           data=None, **kwargs)

    def require_dataset(self, name, shape, dtype, exact=False, **kwargs):
        return self.h5group.require_dataset(name, shape, dtype, exact=exact,
                                            **kwargs)

    def __getitem__(self, key):
        if not key in self.h5group:
            return self.h5group[key]  # let h5py raise its own exception
        if isinstance(self.h5group[key], h5py.Group):
            return Group(self.h5group[key])
        else:
            dset = self.h5group[key]
            if self.return_value:
                if self.unpickle and isinstance(dset.value, basestring):
                    try:
                        return cPickle.loads(dset.value)
                    except:
                        return dset.value
                else:
                    return dset.value
            else:
                return dset

    def get(self, name, default=None, getclass=False, getlink=False):
        return self.h5group.get(name, default=default, getclass=getclass,
                                getlink=getlink)

    def __setitem__(self, key, obj):
        try:
            self.h5group.create_dataset(key, data=obj)
        except (ValueError, TypeError):
            if self.pickle:
                self.h5group.create_dataset(key, data=cPickle.dumps(obj))
            else:
                raise
        else:
            if type(self.h5group[key].value) is not type(obj) and self.pickle:
                del self.h5group[key]
                self.h5group.create_dataset(key, data=cPickle.dumps(obj))

    def __delitem__(self, key):
        del self.h5group[key]

    def __len__(self):
        return len(self.h5group)

    def __iter__(self):
        return iter(self.h5group)

    def __contains__(self, name):
        return name in self.h5group

    def copy(self, source, dest, name=None):
        self.h5group.copy(source, dest, name=name)

    def visit(self, func):
        return self.h5group.visit(func)

    def visititems(self, func):
        return self.h5group.visititems(func)

    def __repr__(self):
        return repr(self.h5group)


class File(Group):
    """Wrapper for h5py.File, using serialization for objects that normally
    would not be able to be stored in a HDF5 file."""
    __created__ = '2013-07-07'
    __modified__ = '2013-07-18'
    # former h5obj.File (developed 2012-06-07 until 2013-03-05)

    def __init__(self, name, mode=None, driver=None, libver=None, pickle=True,
                 unpickle=True, return_value=True, **kwargs):
        self.h5group = h5py.File(name, mode=mode, driver=driver, libver=libver,
                                 **kwargs)
        self.pickle = pickle
        self.unpickle = unpickle
        self.return_value = return_value

    @property
    def attrs(self):
        return self.h5group.attrs

    @property
    def filename(self):
        return self.h5group.filename

    @property
    def driver(self):
        return self.h5group.driver

    @property
    def mode(self):
        return self.h5group.mode

    @property
    def fid(self):
        return self.h5group.fid

    @property
    def libver(self):
        return self.h5group.libver

    def close(self):
        self.h5group.close()

    def flush(self):
        return self.h5group.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()
