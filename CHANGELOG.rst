===============
SHED Change Log
===============

.. current developments

v0.7.2
====================

**Fixed:**

* ``original_time`` metadata goes to ``original_start_time`` so file saving 
  works properly



v0.7.1
====================

**Added:**

* ``shed.simple.simple_to_event_stream_new_api`` which has a new API for
  describing the data which is going to be converted to the event model

**Changed:**

* ``AlignEventStreams`` clears buffer on stop docs not start



v0.7.0
====================

**Added:**

* Notebook examples
* ``shed.simple.LastCache`` a stream node type which allows for caching the last
  event and emiting it under its own descriptor
* Merkle tree like hashing capability for checking if two pipelies are the same

**Changed:**

* ``replay.rebuild_node`` creates ``Placeholder`` streams so that we can build 
  the pipeline properly.
* If no ``stream_name`` is provided for ``SimpleFromEventStream`` then a name 
  is produced from the ``data_address``.
* ``translation.FromEventStream`` now captures environment information via an 
  iterable of funciton calls, it defaults to capturing the conda environment
* ``translation.ToEventModel`` issues a ``RuntimeError`` if the pipeline 
  contains a lambda function, as they are not capturable.

**Removed:**

* ``replay.replay`` ``export`` kwarg. We no longer auto add data to a databroker

**Fixed:**

* check for hashability before checking if in graph



v0.6.3
====================

**Added:**

* Start documents now have their own ``scan_id``

**Changed:**

* Don't validate start documents
* ``SimpleFromEventModel`` nodes give themselves descriptive names if none given

**Fixed:**

* ``AlignEventStream`` properly drops buffers when start docs come in on the
  same buffer



v0.6.2
====================

**Changed:**

* ``AlignEventStream`` now supports stream specific joins



v0.6.1
====================

**Fixed:**

* Flush ``AlignEventStream` which makes certain that even in the event of error
  we have fresh ``AlignEventStream`` buffers



v0.6.0
====================

**Added:**

* descriptor data_keys metadata can be added

**Changed:**

* ``AllignEventStreams`` keeps track of the first map's start uid (for file saving)



v0.5.1
====================

**Fixed:**

* Protect Parallel nodes behind a ``try except``



v0.5.0
====================

**Added:**

* ``examples/best_effort.py`` as an example of using SHED with
  ``BestEffortCallback``.
* ``ToEventStream`` can now take no ``data_keys``. This assumes that the
  incoming data will be a dict and that the keys of the dict are the data keys.

**Changed:**

* Get ``ChainDB`` from xonsh
* Use common ``DocGen`` for document generation
* Exchanged ``zstreamz`` dep for ``rapidz``

**Removed:**

* Removed ``event_streams`` and ``databroker_utils`` and associated tests

**Fixed:**

* Run package level imports so that ``ToEventStream`` and others default to 
  serial
* A ``SimpleToEventStream`` node can now have multple principle nodes
* The same header can be run into a pipeline multiple times
* Multiple principle nodes are now properly handled
* ``AlignEventStreams`` now works with resource and datum docs
* File writers work properly



v0.4.1
====================

**Fixed:**

* ``FromEventStream`` now looks for ``uid`` or ``datum_id``




v0.4.0
====================

**Added:**

* Type mapping for ``ToEventStream``

* Convert ``ChainDB`` to dict


**Fixed:**

* Carve out an if statement for numpy ufuncs to get the numpy module




v0.3.0
====================

**Changed:**

* Readme now reflects the current design architecture

* Provenance example is now in the examples folder

* ``hash_or_uid`` is now ``_hash_or_uid``


**Deprecated:**

* ``EventStream`` nodes in favor of ``streamz`` nodes and ``translation`` nodes


**Fixed:**

* ``ToEventStream`` now tracks the time that data was received

* ``ToEventStream`` is now executed before the rest of the graph so graph times
  match the execution time.




v0.2.1
====================

**Added:**

* conda forge activity to rever

* template back to news




v0.2.0
====================

**Added:**

* Nodes for Databroker integration
* Setup Rever changelog


**Fixed:**

* Fixed the tests after the move to `ophyd.sim` from `bluesky.examples`




