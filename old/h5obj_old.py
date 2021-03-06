#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This module wraps h5py to be able to store almost any native Python type,
as well as instances of user-defined classes, in a HDF5 file. The list of
supported data structures includes:

- list, tuple, set, frozenset (may also be nested in any possible way, and
  may also be empty)
- dict
- int, float, long, complex
- str

User-defined classes (to be particular: Those variables that possess an
attribute named "__module__") are serialized using the cPickle module and
then stored as a normal string into the HDF5 file. To restore the objects,
the module where the class definition exists must be present at exactly the
same place (and under the same name) like when the object was written to the
HDF5 file (this is a requirement of the module cPickle to work correctly).

The created HDF5 datasets and groups get special attributes to retrieve the
right type of each Python object (__DTYPE__, __NTYPE__) and to store
information about a user-defined class (__CLASS__, __MODULE__), so that the
object can be reconstructed later. These attributes must of course not be
overwritten or deleted by any 3rd party software, otherwise the objects may
not be restored in the right format (cannot be casted to the right type).

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


def obj2hdf(__f, **objects):
  """Store objects in a HDF5 file, referenced by the h5py.File object f.
  Depending on the object type, it either just creates a suitable HDF5
  dataset, or it may create multiple nested HDF5 groups, together with
  certain attributes and datasets, to represent the Python object.
  name=object value pairs have to be specified using keyword arguments."""
  __created__ = '2011-11-15'
  __modified__ = '2012-12-11'
  # former h5obj._create_obj from 2011-10-21 until 2011-10-25
  # former tb.savehdf from 2011-02-05 until 2011-03-30

  # loop over all given names and data objects
  for name, data in objects.iteritems():
    # get types
    dtype = type(data)
    ntype = type(name)

    # initialize pickle state
    pickled = False

    # overwrite existing dataset
    if name in __f:
      del(__f[name])

    # distinguish object types
    if data is None:
      # None type
      new = __f.create_dataset(name, data='__NONE__')
    elif dtype is bool:
      # boolean type
      new = __f.create_dataset(name, data=data)
    elif dtype in (int, float, long, complex):
      # built-in numeric types
      new = __f.create_dataset(name, data=data)
    elif dtype is str: ### what about unicode? use basestring?
      # string
      new = __f.create_dataset(name, data=data)
    elif dtype in (list, tuple):
      if len(data) == 0:
        new = __f.create_dataset(name, data='__EMPTY__')
      elif _allscalar(data) and _equaltype(data) and not _anyobject(data):
        # then, a normal dataset can be used
        try:
          new = __f.create_dataset(name, data=data)
        except TypeError:
          # fix saving lists of None values
          new = __f.create_group(name)
          digits = len(str(len(data)-1)) if len(data) > 0 else 1
          keys = ['key%0*.*i' \
                  % ((digits,)*2+(k,)) for k in xrange(len(data))]
          obj2hdf(new, **dict(zip(keys, data)))
      else:
        # a HDF5 group has to be used to store the sequence
        new = __f.create_group(name)
        digits = len(str(len(data)-1)) if len(data) > 0 else 1
        keys = ['key%0*.*i' % ((digits,)*2+(k,)) for k in xrange(len(data))]
        obj2hdf(new, **dict(zip(keys, data)))
    elif dtype in (set, frozenset):
      if len(data) == 0:
        new = __f.create_dataset(name, data='__EMPTY__')
      elif _allscalar(data) and _equaltype(data):
        ### why not check anyobject here?
        new = __f.create_dataset(name, data=tuple(data))
      else:
        # use HDF5 group
        new = __f.create_group(name)
        digits = len(str(len(data)-1)) if len(data) > 0 else 1
        keys = ['key%0*.*i' % ((digits,)*2+(k,)) % k for k \
                                                      in xrange(len(data))]
        obj2hdf(new, **dict(zip(keys, tuple(data))))
    elif dtype is dict:
      new = __f.create_group(name)
      obj2hdf(new, **data)
    elif dtype in [scipy.ndarray, scipy.matrix]:
      if 0 in data.shape:
        # empty array or matrix. Save string representation
        new = __f.create_dataset(name, data='__EMPTY__')
      else:
        new = __f.create_dataset(name, data=data)
    elif scipy.sparse.issparse(data):
      # the classes could simply be serialized, but then, other people would
      # have no chance to read the matrices with their own programs
      if dtype is not scipy.sparse.csr_matrix:
        # convert to csr_matrix:
        data = data.tocsr()
      new = __f.create_group(name)
      obj2hdf(new, data=data.data, indices=data.indices, indptr=data.indptr,
              shape=data.shape, dtype=dtype)
    elif dtype in (bundle.Bundle, structure.struct):
      # only with Bundle it is possible to use a group, not with dict,
      # because dict may have non-string keys. So, dict has to be pickled
      # structure.struct was replaced by bundle.Bundle and is only supported
      # here for backwards-compatibility
      #dtype = bundle.Bundle
      if name in __f:
        del(f[name]) # make sure to overwrite
      new = __f.create_group(name)
      obj2hdf(new, **data)
    elif dtype is cofunc.coFunc:
      # store continuous function objects as groups, so that their data is
      # still accessible using standard methods (not pickled)
      new = __f.create_group(name)
      obj2hdf(new, x=data.x, y=data.y, attrs=data.attrs)
    elif dtype is cofunc.coFunc2d:
      # store continuous 2D function objects as groups, so that their data is
      # still accessible using standard methods (not pickled)
      new = __f.create_group(name)
      obj2hdf(new, x=data.x, y=data.y, z=data.z, attrs=data.attrs)
    else:
      # when all other means fail, serialize the object using cPickle
      #print name, type(name), data, type(data)
      new = __f.create_dataset(name, data=cPickle.dumps(data))
      pickled = True

    # always store types and current date and time in form of HDF5 attributes
    new.attrs.create('__DTYPE__', dtype.__name__)
    new.attrs.create('__NTYPE__', ntype.__name__)
    new.attrs.create('DATE', time.ctime())
    new.attrs.create('__PICKLED__', pickled)

    # if the object has attributes named __module__ or __class__, store them
    # they could be important to recreate objects of user-defined classes
    try:
      __f.attrs.create('__MODULE__', data.__module__)
    except AttributeError:
      pass
    try:
      __f.attrs.create('__CLASS__', data.__class__.__name__)
    except AttributeError:
      pass
















def hdf2obj(f, *names):
  """Read Python objects that are stored in a HDF5 file, referenced by the
  h5py.File object identifier f."""
  __created__ = '2011-11-15'
  __modified__ = '2012-11-05'
  # former h5obj._get_obj from 2011-10-25
  # former tb.savehdf from 2011-02-05 until 2011-03-30

  # initialize value array
  values = []

  for name in names:
    if name not in f:
      raise KeyError,\
        'dataset or group "%s" not found in HDF5 file object' % name

    # get object
    obj = f[name]

    # load attributes
    dtype     = obj.attrs.get('__DTYPE__',   None)
    ntype     = obj.attrs.get('__NTYPE__',   None)
    module    = obj.attrs.get('__MODULE__',  None)
    classname = obj.attrs.get('__CLASS__',   None)
    pickled   = obj.attrs.get('__PICKLED__', False)

    # distinguish data types
    if pickled:
      # then it is easy: just unpickle the object
      try:
        value = cPickle.loads(obj.value)
      except ValueError:
        filename = getattr(f, 'filename', f.file.filename)
        raise ValueError, 'cannot unpickle data "%s" from file "%s"' \
                          % (name, filename)
    elif dtype == 'NoneType':
      value = None
    elif dtype == 'bool':
      value = obj.value
    elif dtype == 'int':
      value = int(obj.value)
    elif dtype == 'long':
      value = long(obj.value)
    elif dtype == 'float':
      value = float(obj.value)
    elif dtype == 'complex':
      value = complex(obj.value)
    elif dtype == 'str':
      value = str(obj.value)
    elif dtype == 'list':
      if type(obj).__name__ == 'Group':
        keys = obj.keys()
        keys.sort()
        value = []
        for key in keys:
          value.append(hdf2obj(obj, key))
      elif isinstance(obj.value, basestring) and obj.value == '__EMPTY__':
        value = []
      else:
        # plain lists that were saved as a normal 1D array dataset
        value = list(obj.value)
    elif dtype == 'tuple':
      if type(obj).__name__ == 'Group':
        keys = obj.keys()
        keys.sort()
        value = []
        for key in keys:
          value.append(hdf2obj(obj, key))
        value = tuple(value)
      elif isinstance(obj.value, basestring) and obj.value == '__EMPTY__':
        value = ()
      else:
        # plain tuples that were saved as a normal 1D array dataset
        value = tuple(obj.value)
    elif dtype == 'set':
      if type(obj).__name__ == 'Group':
        keys = obj.keys()
        value = []
        for key in keys:
          value.append(hdf2obj(obj, key))
        value = set(value)
      elif isinstance(obj.value, basestring) and obj.value == '__EMPTY__':
        # empty sets
        value = set()
      else:
        # sets that were saved as a 1D ndarray
        value = set(obj.value)
    elif dtype == 'frozenset':
      if type(obj).__name__ == 'Group':
        keys = obj.keys()
        value = []
        for key in keys:
          value.append(hdf2obj(obj, key))
        value = frozenset(value)
      elif isinstance(obj.value, basestring) and obj.value == '__EMPTY__':
        # empty frozensets
        value = frozenset()
      else:
        # frozensets that were saved as a 1D ndarray
        value = frozenset(obj.value)
    elif dtype in ['struct', 'Bundle']:
      value = bundle.Bundle()
      for key in obj.iterkeys():
        value[key] = hdf2obj(obj, key)
    elif dtype == 'dict':
      if isinstance(obj, h5py.Group):
        value = {}
        for key in obj.iterkeys():
          value[key] = hdf2obj(obj, key)
      else:
        value = cPickle.loads(obj.value)

      #value = {}
      #for key in obj.iterkeys():
        #ntype = obj[key].attrs.get('__NTYPE__', 'str')
        #if ntype == 'str':
          #value[key] = obj.get_obj(key)
        #else:
          #value[eval(key)] = obj.get_obj(key)
    elif dtype == 'ndarray':
      if isinstance(obj.value, basestring) and obj.value == '__EMPTY__':
        value = scipy.array([], dtype=scipy.float64)
      else:
        value = scipy.array(obj.value)
    elif dtype == 'matrix':
      if isinstance(obj.value, basestring) and obj.value == '__EMPTY__':
        value = scipy.matrix([], dtype=scipy.float64)
      else:
        value = scipy.matrix(obj.value)
    elif dtype in ('csc_matrix', 'csr_matrix', 'bsr_matrix', 'lil_matrix',
                   'dok_matrix', 'coo_matrix', 'dia_matrix'):
      # sparse matrices are always saved in CSR format
      # convert back to original format
      shape   = hdf2obj(obj, 'shape')
      data    = hdf2obj(obj, 'data')
      indices = hdf2obj(obj, 'indices')
      indptr  = hdf2obj(obj, 'indptr')
      value = scipy.sparse.csr_matrix((data, indices, indptr),
                                      shape=shape).asformat(dtype[:3])
    elif dtype == 'coFunc':
      # load continuous function object
      x     = hdf2obj(obj, 'x')
      y     = hdf2obj(obj, 'y')
      attrs = hdf2obj(obj, 'attrs')
      value = cofunc.coFunc(x=x, y=y, attrs=attrs)
    elif dtype == 'coFunc2d':
      # load continuous function object
      x     = hdf2obj(obj, 'x')
      y     = hdf2obj(obj, 'y')
      z     = hdf2obj(obj, 'z')
      attrs = hdf2obj(obj, 'attrs')
      value = cofunc.coFunc2d(x=x, y=y, z=z, attrs=attrs)
    else:
      # then assume it is just a normal dataset representing whatever it
      # contains. If obj is a HDF5 group, return contents in form of a struct
      if type(obj).__name__ == 'Group':
        value = bundle.Bundle()
        for key in obj.keys():
          value[key] = hdf2obj(obj, key)
      else:
        value = obj.value

    # collect values
    values.append(value)

  # return values
  if len(values) == 1:
    return values[0]
  else:
    return values

















#=============================#
# Functions for type checking #
#=============================#

def _allscalar(seq):
  """Test if all elements of the sequence or set are scalar."""
  # 2011-09-13
  # former tb.allscalar from 2011-03-30
  for element in seq:
    if _isiterable(element):
      return False
    return True

def _anyobject(seq):
  """Test if any element of the sequence or set is an instance of a
  user-defined class."""
  # 2011-09-13
  # former tb.anyobject from 2011-03-30
  for element in seq:
    if _isobject(element):
      return True
  return False

def _equaltype(seq):
  """Test if all elements of the sequence or set have the same type."""
  # 2011-09-13
  # former tb.equaltype from 2011-03-30
  seq = list(seq)
  first = seq.pop()
  for element in seq:
    if type(element).__name__ != type(first).__name__:
      return False
  return True

def _isiterable(obj):
  """Check if an object is iterable. Return True for lists, tuples,
  dictionaries and numpy arrays (all objects that possess an __iter__
  method). Return False for scalars (float, int, etc.), strings, bool and
  None."""
  # 2011-09-13
  # former tb.isiterable from 2011-01-27
  # former mytools.isiterable
  # Inicial idea from
  # http://bytes.com/topic/python/answers/514838-how-test-if-object-sequence-
  # iterable:
  # return isinstance(obj, basestring) or getattr(obj, '__iter__', False)
  # I found this to be better:
  return not getattr(obj, '__iter__', False) == False

def _isobject(obj):
  """Return True if obj possesses an attribute called "__dict__", otherwise
  return False."""
  # 2011-09-13
  # former tb.isobject from 2011-02-09
  # former mytools.isobject
  return not getattr(obj, '__dict__', False) == False













#===================#
# h5py.File wrapper #
#===================#



class File(collections.MutableMapping):
  """Wraps "h5py.File" to provide attribute-like access to the contents of
  the HDF5 file, plus uses the function "hdf2obj" and "obj2hdf" to be able to
  store any Python object (partly using the module "cPickle").

  To do:
  --> in File, offer access to attributes of (dict-like) HDF5 datasets
      (example: "__param__.shape") (support at least dict and Bundle objects)
      (what if attribute is a function? try to execute the function? Think
      about "__scell__.tbmat" ...)
  --> enable HDF5 group handling, allow slash in dataset names (dataset
      paths)
  --> combination of both: allow things like "grp/dset.attr" """
  __created__ = '2012-06-07'
  __modified__ = '2013-03-05'
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

  def create_dataset(self, name, shape=None, dtype=None, data=None,
                     **kwargs):
    """Create a new HDF5 dataset."""
    __created__ = '2012-09-12'
    return self.h5file.create_dataset(name, shape=None, dtype=None,
                                      data=None, **kwargs)

  def dsetkeys(self):
    """Get the keys of all datasets of the file."""
    __created__ = '2013-01-16'
    __modified__ = '2013-01-16'

    dsetnames = []
    def func(key, obj):
      #print key,
      #print key, obj.parent, obj.parent.attrs.keys()
      #print obj.attrs.keys(), obj.parent.attrs.keys(),
      if '__DTYPE__' in obj.attrs and not '__DTYPE__' in obj.parent.attrs:
                                            #isinstance(obj, h5py.Group)
        dsetnames.append(key)
        #print 'is dset',
      #print
    try:
      self.h5file.visititems(func)
    except RuntimeError:
      pass
    return dsetnames
