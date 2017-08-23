=========================
Invenio-matcher benchmark
=========================

This repository contains test data and scripts in order to evaluate the accuracy of https://github.com/inveniosoftware-contrib/invenio-matcher with INSPIRE data.

How to run the benchmark
========================

First, make sure your system has installed **docker** and **docker-compose**.

Get the latest version of the Docker images:

.. code-block:: console

    $ docker-compose pull

Start all the Docker containers:

.. code-block:: console

    $ docker-compose up -d

Run the benchmark:

.. code-block:: console

    $ docker-compose run --rm web python match.py publisher_updates --output

After running all the files present in the folder `publisher_updates`, a final output will be produced:


    #### STATS ####

    Total analyzed:  2258

    True positives:  1928

    False positives:  7

    True negatives:  0

    False negatives:  323
    
    Duplicate exact match:  0

    Precision:  0.996382428941
    
    Recall:  0.85650821857

    F1 Score:  0.921165790731

