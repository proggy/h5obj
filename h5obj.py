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
"""This module defines a wrapper for the class h5py.File (and h5py.Group),
and adds to it the ability to store any Python object (including nested and
empty lists and tuples, None, user-defined objects etc.) using the concept of
serialization. It uses the cPickle module for objects which fail to convert
properly to a default HDF5 datatype, and when loading any string data, it
tries to deserialize it before loading the string normally. It also makes
sure that if you get back exactly the datatype that you were saving (e.g., if
you save a tuple or list using h5py.File, you probably get back a
numpy.ndarray).

To do:
--> make get-method unpickle (so far it just calls the original get-method)
"""
__created__  = '2013-07-07'
__modified__ = '2013-07-07'
__version__  = '0.1'
# former h5obj (developed 2011-10-21 until 2013-03-14)
import collections, cPickle, h5py




class Group(collections.MutableMapping):
  """WWrapper for h5py.Group, using serialization for objects that normally
  would not be able to be stored in a HDF5 file."""
  __created__ = '2013-07-07'
  __modified__ = '2013-07-07'

  def __init__(self, h5group):
    self.h5group = h5group

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

  def __getitem__(self, key, unpickle=True):
    if not key in self.h5group:
      return self.h5group[key] # let h5py raise its own exception
    if isinstance(self.h5group[key], h5py.Group):
      return Group(self.h5group[key])
    else:
      data = self.h5group[key].value
      if isinstance(data, basestring) and unpickle:
        try: data = cPickle.loads(data)
        except: pass
      return data

  def get(self, name, default=None, getclass=False, getlink=False):
    return self.h5group.get(name, default=default, getclass=getclass,
                            getlink=getlink)

  def __setitem__(self, key, obj, pickle=True):
    try: self.h5group[key] = obj
    except ValueError:
      if pickle:
        self.h5group[key] = cPickle.dumps(obj)
      else:
        raise
    else:
      if type(self.h5group[key]) is not type(obj) and pickle:
        del self.h5group[key]
        self.h5group[key] = cPickle.dumps(obj)

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
  __modified__ = '2013-07-07'
  # former h5obj.File (developed 2012-06-07 until 2013-03-05)

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

  def __init__(self, *args, **kwargs):
    self.h5group = h5py.File(*args, **kwargs)

  def close(self):
    self.h5group.close()

  def flush(self):
    return self.h5group.flush()

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, exc_traceback):
      self.close()