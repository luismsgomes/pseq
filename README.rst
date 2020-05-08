Parallel Sequence Processing
============================

This package provides a framework for parallel processing of sequences.

Copyright ® 2019, Luís Gomes luismsgomes@gmail.com. All rights reserved.

Links: https://github.com/luismsgomes/pseq | https://pypi.org/project/pseq/

There are four main concepts in this package, each one modeled as a
class:

-  A ``WorkUnit`` represents a sequence element.
-  A ``Producer`` is responsible for generating a sequence of
   ``WorkUnit``\ s to be processed.
-  A ``Processor`` is responsible for processing ``WorkUnits``
   individually.
-  A ``Consumer`` is responsible for handling processed ``WorkUnits``.

These concepts/objects are the building blocks of a
``ParallelSequenceProcessor``, which is depicted below.

::

                                 .-------------.
                              .->| Processor 1 |--.
                              |  `-------------'  |
    .----------.  .-------.  /   .-------------.   \   .------.  .----------.
    | Producer |->| Queue |-+--->| Processor 2 |----+->| Hold |->| Consumer |
    `----------'  `-------'  \   `-------------'   /   `------'  `----------'
                              |       ...         |
                              |  .-------------.  |
                              `->| Processor k |--'
                                 `-------------'

A ``ParallelSequenceProcessor`` coordinates one ``Producer``, one
``Consumer`` and ``k`` ``Processor``\ s.

Unless specified otherwise, the ``Consumer`` receives processed
``WorkUnit``\ s in the same order as produced by the ``Producer``. This
ordering is ensured by holding processed ``WorkUnit``\ s until all units
produced before them have been processed and passed onto the
``Consumer``.

Usage
-----

Currently, the only documentation available is the example code provided 
in ``example.py``.


License
-------

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License version 3 as published
by the Free Software Foundation.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License version 3 for more details.

You should have received a copy of the GNU General Public License along
with this library; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
