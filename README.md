# redsky
[![Build Status](https://travis-ci.org/xpdAcq/redsky.svg?branch=master)](https://travis-ci.org/xpdAcq/redsky)
[![codecov](https://codecov.io/gh/xpdAcq/redsky/branch/master/graph/badge.svg)](https://codecov.io/gh/xpdAcq/redsky)
[![Code Health](https://landscape.io/github/xpdAcq/redsky/master/landscape.svg?style=flat)](https://landscape.io/github/xpdAcq/redsky/master)

a data processing interface

<img src="https://github.com/xpdAcq/redsky/blob/master/examples/mystream.png" style="width: 200px;"/>


## Current Design/Architecture
Here are some rough observations/axioms that may prove useful for design and/or
discussion. (Note that this is restricted to the application of streaming to
the document model)

1. All stream nodes should emit in the document model. 
This makes certain that:
   1. the nodes are fairly coarse in their operation (not that this is stopping the
underlying code from being complex)
   1. the results can be passed between nodes without worry that the data will 
   be in some state of undress (we'll always know what we are getting as input 
   data topology)
   1. most of the dat can be inserted into MDS/FS without too much work
1. We need to expand the document model (not the MDS/FS schema) to include some
sort of tuple of documents so we can support zip. 
The two contenders which come to my mind are (Note that neither of these can
be entered into MDS/FS but that is ok because we shouldn't be modifying the
data and zipping them together in the same step):
   1. ((name, doc), (name, doc)) which is what you would get from a direct zip
   of the pairs
   1. (name, (doc, doc)) which is more compact (since we demand that docs 
   emitted together have same name) and may be more friendly to a callback like
   interface (note that the callback interface asks for only the docs on the 
   processing level)
1. Thou shalt not mix documents, we can't emit starts with anything other
than starts.
1. It seems that start, descriptor, and stop documents can be treated 
similarly. 
We can just issue new ones (in the event that we performed some 
operation on the data) or just let the existing ones pass through (if we are
zipping or some other non-data generating operation).
1. Event documents however require a very different procedure, since they are
the ones most likely to be modified or otherwise operated upon.
Events seem to have three fundamental operations
   1. Return the "guts" of the event: providing the data inside the event, as
   specified at init time, to other functions (eg filter, map functions, 
   etc)
   1. Ingest new data and issue a new event: take in the outputs of a 
   scientific function and put them into a new event, using a map between the
   function output and the data keys provided at init time.
   1. Pass the event through (this is the least likely to be used)

Scientific function: A function which operates on arrays, and other basic
python types (`ints`, `floats`, etc.). This function should return arrays and
basic python types. These functions should handle most of the heavy lifting
of the problem including how to parallelize the problem, how to access 
non-local compute resources, etc.