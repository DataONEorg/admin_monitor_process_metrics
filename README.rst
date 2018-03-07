admin_monitor_process_metrics
=============================

Tail cn-process-metric.log and report metrics.

Python 2/3 script to tail the ``/var/log/dataone/daemon/cn-process-metric.log`` 
and report the current state of values to:

* A JSON file at ``/var/www/processing_metrics.json``
* A plain text file at ``/var/www/processing_metrics.txt``
* Metrics emitted to measure-unm-1.dataone.org over UDP to the statsd receiver.

The script maintains state between runs using a log offset file 
(``cn-process-metric.log.offset``)and the ``processing_metrics.json`` file and
so is very efficient after the first run.

The script is run by cron every minute via an entry in ``/etc/cron.d``


Installation
------------

Install `pygtail <https://pypi.python.org/pypi/pygtail>`_ by::

  pip install -U pygtail

Copy the ``tail_processing_metrics.py`` python and ``tail_processing_metrics`` 
bash script to ``/usr/local/bin``. 

Make ``tail_processing_metrics`` executable.

Create an entry in ``cron.d``::

  # Process the d1-processing metrics log and
  # place output in a web accessible location
  #
  * * * * * root /usr/local/bin/tail_processing_metrics

Verify execution by::

  sudo /usr/local/bin/tail_processing_metrics

The files ``processing_metrics.json`` and ``processing_metrics.txt`` should be
present in ``/var/www``.

