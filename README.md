# redsky
[![Build Status](https://travis-ci.org/xpdAcq/redsky.svg?branch=master)](https://travis-ci.org/xpdAcq/redsky)
[![codecov](https://codecov.io/gh/xpdAcq/redsky/branch/master/graph/badge.svg)](https://codecov.io/gh/xpdAcq/redsky)
[![Code Health](https://landscape.io/github/xpdAcq/redsky/master/landscape.svg?style=flat)](https://landscape.io/github/xpdAcq/redsky/master)

a data processing interface

## Current Design/Architecture
Currently there are three parts/layers to the redsky design.
From inner most to outer most:
1. Scientific function: A function which operates on arrays, and other basic
python types (`ints`, `floats`, etc.). This function should return arrays and
basic python types. These functions should handle most of the heavy lifting
of the problem including how to parallelize the problem, how to access 
non-local compute resources, etc.
1. Header-Function Interface (HFI): This function:
   1. Take in one or more streams
   1. `yields` start, descriptor, event, and stop dictionaries with their name
   in the form of `yield 'document_name', dict`
   1. Maps values from the event documents to args/kwargs in the scientific 
   function.
   1. Executes the scientific function
   1. Capture exception information
1. `db_store` decorator: This decorator handles the interface between the 
yielded documents and the databases in which they should be stored, this 
includes:
   1. How to issue documents to metadatastore.
   1. How to mutate documents for filestore, while yielding unchanged documents
   to HFIs down the chain.
   1. How to save the data in the documents which needs to be saved on disk and
   how to tell filestore about their existance.