#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This module wraps h5py to be able to store almost any native Python type, as
well as instances of user-defined classes, in a HDF5 file. The list of
supported data structures includes:

- list, tuple, set, frozenset (may also be nested in any possible way, and  may
  also be empty)
- dict
- int, float, long, complex
- str

User-defined classes (to be particular: Those variables that possess an
attribute named "__module__") are serialized using the cPickle module and then
stored as a normal string into the HDF5 file. To restore the objects, the
module where the class definition exists must be present at exactly the same
place (and under the same name) like when the object was written to the HDF5
file (this is a requirement of the module cPickle to work correctly).

The created HDF5 datasets and groups get special attributes to retrieve the
right type of each Python object (__DTYPE__, __NTYPE__) and to store information
about a user-defined class (__CLASS__, __MODULE__), so that the object can be
reconstructed later. These attributes must of course not be overwritten or
deleted by any 3rd party software, otherwise the objects may not be restored
in the right format (cannot be casted to the right type).

Additionally, each created HDF5 dataset or group will get an attribute named
__DATE__, containing a current time stamp of the time when the object was
written to the HDF5 file.

To do:
--> in File, offer access to attributes of (dict-like) HDF5 datasets
    (example: "__param__.shape") (support at least dict, Bundle, objects)
--> enable HDF5 group handling, allow slash in dataset names (dataset paths)
--> combination of both: allow things like "grp/dset.attr"

Written by Daniel Jung, Jacobs University Bremen, Germany (2011-2013).
"""
__created__  = '2011-10-21'
__modified__ = '2013-03-14'
import collections, cPickle, h5py, scipy, sys, time
import bundle, cofunc, structure
#from tb.misc import dump as _dump


def saveobj(f, name, obj, overwrite=True):
  """Save the given Python object to the HDF5 file object f, in form of a
  dataset with the given name. If the object cannot be represented by a HDF5
  datatype, serialize the object using the module cPickle.

  Up to three attributes may be saved with the dataset. They are necessary to
  be able to restore the Python object: "__type__", "__itemtypes__" and
  "__pickled__"."""
  __created__ = '2013-03-14'
  __modified__ = '2013-03-14'
  if overwrite and name in f: del(f[name]) # overwrite
  dtype = type(obj)
  try:
    dset = f.create_dataset(name, data=obj)
    dset.attrs['__pickled__'] = False
    if dtype in (list, tuple) and hetero(obj):
      dset.attrs['__itemtypes__'] = ','.join([repr(type(item)) for item in obj])
  except ValueError:
    dset = f.create_dataset(name, data=cPickle.dumps(obj))
    dset.attrs['__pickled__'] = True
  dset.attrs['__type__'] = repr(type(obj))


def loadobj(f, name):
  """Restore a Python object from a dataset with the given name, present the
  given HDF5 file object f.

  The Python object must have been saved using the function "saveobj". All
  attributes created by that function ("__type__", "__itemtypes__" or
  "__pickled__") are needed to restore the Python object correctly."""
  __created__ = '2013-03-14'
  __modified__ = '2013-03-14'
  obj = f[name].value
  if f[name].attrs.get('__pickled__', False):
    obj = cPickle.loads(obj)
  originaltype = eval(f[name].attrs.get('__type__', None))
  if originaltype and type(obj) is not originaltype:
    obj = originaltype(obj)
  return obj


def hetero(sequence):
  """Check if the given sequence (list or tuple) contains heterogeneous data,
  i.e. not all of the items have the same type."""
  __created__ = '2013-03-14'
  __modified__ = '2013-03-14'
  if len(sequence) == 0: return False
  type1 = type(sequence[0])
  for item in sequence[1:]:
    if type(item) is not type1:
      return True
  return False




#===================#
# h5py.File wrapper #
#===================#


class File(collections.MutableMapping):
  """Wraps "h5py.File" to provide attribute-like access to the contents of the
  HDF5 file, using the functions "saveobj" and "loadobj" to be able to store
  any Python object (serializing it if neccessary)."""
  __created__ = '2012-06-07'
  __modified__ = '2013-03-14'

  def __init__(self, *args, **kwargs):
    try:
      f = h5py.File(*args, **kwargs)
    except IOError:
      raise IOError, 'unable to open file "%s"' % args[0]
    collections.MutableMapping.__setattr__(self, 'h5file', f)

    collections.MutableMapping.__setattr__(self, '_closed', False)

    # provide filename
    collections.MutableMapping.__setattr__(self, 'filename',
                                           self.h5file.filename)

  def __getattr__(self, name):
    return self[name]

  def __getitem__(self, key):
    # respect special keys to get the filename of the HDF5 file
    if key in ('filename', '__filename__', '@'):
      return str(self.h5file.filename)
    else:
      return hdf2obj(self.h5file, key)

  def __setattr__(self, name, value):
    self[name] = value

  def __setitem__(self, key, value):
    obj2hdf(self.h5file, **{key: value})

  def __delattr__(self, name):
    del self.h5file[name]

  def __delitem__(self, key):
    del self.h5file[key]

  def __iter__(self):
    return iter(self.h5file)

  def __len__(self):
    return len(self.h5file)

  def close(self):
    """Close the HDF5 file if it is not already closed."""
    __modified__ = '2012-08-25'
    if self.h5file and not self._closed:
      self.h5file.close()
      collections.MutableMapping.__setattr__(self, '_closed', True)

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, exc_traceback):
      self.close()

  #def __del__(self):
    #self.close()
    ### leads to errors if the file is already closed

  def __repr__(self):
    return repr(self.h5file)

  def create_group(self, name):
    """Create a new HDF5 group."""
    __created__ = '2012-09-12'
    return self.h5file.create_group(name)

  def create_dataset(self, name, shape=None, dtype=None, data=None, **kwargs):
    """Create a new HDF5 dataset."""
    __created__ = '2012-09-12'
    return self.h5file.create_dataset(name, shape=None, dtype=None, data=None,
                                      **kwargs)

  def dsetkeys(self):
    """Get the keys of all datasets of the file."""
    __created__ = '2013-01-16'
    __modified__ = '2013-01-16'

    dsetnames = []
    def func(key, obj):
      if '__DTYPE__' in obj.attrs and not '__DTYPE__' in obj.parent.attrs: #isinstance(obj, h5py.Group)
        dsetnames.append(key)
    try:
      self.h5file.visititems(func)
    except RuntimeError:
      pass
    return dsetnames
