=========================
Invenio-matcher benchmark
=========================

This repository contains test data and scripts in order to evaluate the accuracy of https://github.com/inveniosoftware-contrib/invenio-matcher with INSPIRE data.

How to run the script
=====================

The file `match.py` is the CLI to run the tests.

.. code-block:: bash
    python match_file.py -h                                                                                                  

    usage: match_file.py [-h] [--output] names [names ...]

    Test invenio-matcher.

    positional arguments:
        names       File(s) or directory(ies) to run matcher against.

        optional arguments:
            -h, --help  show this help message and exit
                --output    Output files with false positives and false negatives

And the output provided:

```
#### STATS ####
Total analyzed:  2258
True positives:  1616
False positives:  12
True negatives:  0
False negatives:  630
Duplicate exact match:  0
------------------------
Precision:  0.992628992629
Recall:  0.719501335708
F1 Score:  0.834279814146
```
