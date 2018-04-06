# SHED
[![Build Status](https://travis-ci.org/xpdAcq/SHED.svg?branch=master)](https://travis-ci.org/xpdAcq/SHED)
[![codecov](https://codecov.io/gh/xpdAcq/SHED/branch/master/graph/badge.svg)](https://codecov.io/gh/xpdAcq/SHED)
[![Code Health](https://landscape.io/github/xpdAcq/SHED/master/landscape.svg?style=flat)](https://landscape.io/github/xpdAcq/SHED/master)

Streaming Heterogeneous Event Data

<img src="https://github.com/xpdAcq/SHED/blob/master/examples/mystream.png" style="width: 400px;"/>


## Current Design/Architecture
1. The tooling for the event model management should be as transparent and
small as possible.
   1. SHED accomplishes this by having only two additional nodes 
   ``FromEventStream`` and ``ToEventStream``, which convert data from the 
   event model to base types/numpy and from base types/numpy to the event model
   1. Everything else will be handled by ``streamz`` nodes operating on base
   types and numpy
1. We should track the data provenance with as little burden on the user
as possible.
   1. Since the users have agreed to be part of our ``streamz`` based
   ecosystem we should track data provenance without any additional work on
   the user's part.
   1. This is accomplished by having the translation nodes keep track of the 
      1. source of the data coming into the graph
      1. when the data entered the graph
      1. the graph itself
   1. Data provenance should support:
      1. Replaying data analysis
      1. Env tracking
      1. Playing new data through old analysis
      1. Editing analysis and replaying
1. Data should be stored via a `DataBroker`, which has a similar structure
to the experimental data.
