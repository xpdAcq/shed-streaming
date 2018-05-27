===============
SHED Change Log
===============

.. current developments

v0.2.3
====================

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




