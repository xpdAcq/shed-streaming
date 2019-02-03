===============
SHED Change Log
===============

.. current developments

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




