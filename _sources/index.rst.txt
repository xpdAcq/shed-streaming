.. SHED documentation master file, created by
   sphinx-quickstart on Tue Jul 25 16:31:30 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to SHED's documentation!
================================

Streaming Heterogeneous Event Data (SHED) is a system for streaming
data from the event model into a Streamz graph while tracking data
provenance and workflow execution.

Installation
============

Installation using Conda
------------------------

``shed`` is installable via the ``conda`` package manager.
If you don't have Anaconda (or miniconda) installed please follow the instructions
from `Data Carpentry <https://datacarpentry.org/2016-05-29-PyCon/install.html>`_.

With Anaconda or miniconda please enter into a terminal (on Windows Anaconda
ships with a dedicated command prompt) and type

``conda install shed -c conda-forge``

and follow the prompts. This will install the shed software and all of its dependencies.

Installation from Source
------------------------

Fork and clone the `github repository <https://github.com/xpdAcq/SHED>`_.

Open a terminal, change directory to the local repository and run the
following commands:

``bash install.sh``

.. toctree::
   :maxdepth: 4
   :caption: Contents:

   introduction
   shed
   tutorials


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
