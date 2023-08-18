#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright notice
# ----------------
#
# Copyright (C) 2013-2023 Daniel Jung
# Contact: proggy-contact@mailbox.org
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
adds to it the ability to store native Python types (including nested and
empty lists and tuples, the None type, etc.) using string representations in
json format. It uses the json module for objects which would not preserve
their respective type when saved to HDF5, and when loading strings, it
tries to decode it before interpreting it as a usual string.

h5obj makes sure that you get back exactly the datatype that you were saving
to the HDF5 file. This is to prevent the behavior of h5py.File, which for
example returns a numpy.ndarray when storing a tuple or a list.

If in certain situations, the attempt to encode/decode the data is not
intended, it can be switched off using the attributes "encode" and "decode" of
the classes "Group" and "File".

This module depends on the module "h5py". It is available at
"http://www.h5py.org/" under a BSD license.
"""
__version__ = '0.1.0'

import collections.abc
import json
import h5py


class Group(collections.abc.MutableMapping):
    """Wrapper for h5py.Group, using json format for native Python objects so
    those objects can later be retrieved from HDF5 files with their original
    type.
    """
    def __init__(self, h5group, encode=True, decode=True, return_value=True):
        self.h5group = h5group
        self.encode = encode
        self.decode = decode
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
                if self.decode:# and isinstance(dset[()], str): #dset.value
                    try:
                        return json.loads(dset[()])
                    except:
                        return dset[()]
                else:
                    return dset[()]
            else:
                return dset
    
    def get(self, name, default=None, getclass=False, getlink=False):
        return self.h5group.get(name, default=default, getclass=getclass,
                                getlink=getlink)

    def __setitem__(self, key, obj):
        try:
            self.h5group.create_dataset(key, data=obj)
        except (ValueError, TypeError):
            if self.encode:
                s = json.dumps(obj)
                self.h5group.create_dataset(key, data=s)
            else:
                raise
        else:
            # this executes only if no encoding seemed to be necessary but apparently type could
            # not be preserved, so use encoding anyway
            if type(self.h5group[key][()]) is not type(obj) and self.encode: #self.h5group[key].value
                del self.h5group[key]
                s = json.dumps(obj)
                #tid = h5py.h5t.C_S1.copy()
                #tid.set_size(256)
                #H5T_C_S1_256 = h5py.Datatype(tid)
                self.h5group.create_dataset(key, data=s)#, dtype=str) #H5T_C_S1_256

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
    """Wrapper for h5py.File, using json strings for native Python objects so
    those objects can later be retrieved from HDF5 files with their original
    type.
    """
    def __init__(self, name, mode=None, driver=None, libver=None, encode=True,
                 decode=True, return_value=True, **kwargs):
        self.h5group = h5py.File(name, mode=mode, driver=driver, libver=libver,
                                 **kwargs)
        self.encode = encode
        self.decode = decode
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
