'''
Update a single JSON file based on output from the

  /var/log/dataone/daemon/cn-process-metric.log

file which contains a JSON structure per line.

Visit: 

  https://github.com/DataONEorg/admin_monitor_process_metrics

for more info.
'''

import sys
import os
import argparse
import logging
import json
import pygtail
import statsd


class Collator(object):

  def __init__(self, fndest, environment="production"):
    self.fndest = fndest
    self.environment="production"
    self.data = {
      "dateLogged": "",
      "synchronization status": {},
      "replication status": {},
    }
    self.router = {
      'synchronization harvest retrieved': self.addSyncHarvestRetrieved,
      'synchronization harvest submitted': self.addSyncHarvestSubmitted,
      'synchronization queued': self.addSyncQueued,
      'replication status': self.addReplicationStatus,
    }
    self.load()


  def save(self):
    with open(self.fndest, "wt") as fdest:
      json.dump(self.data, fdest, indent=2)


  def load(self):
    if not os.path.exists(self.fndest):
      return
    with open(self.fndest, "rt") as finput:
      self.data = json.load(finput)


  def setLastTimeStamp(self, date_logged):
    self.data['dateLogged'] = date_logged


  def addSyncHarvestRetrieved(self, event, item):
    tag = 'RETRIEVED'
    if event.lower().strip() == "synchronization harvest submitted":
      tag = 'SUBMITTED'
    event = "synchronization status"
    tname = item.pop('threadName')
    tid = item.pop('threadId')
    nodeid = item.pop('nodeId')
    message = item.pop('message')
    self.setLastTimeStamp(item.pop('dateLogged'))
    v = int(message)
    if nodeid not in self.data[event]:
      self.data[event][nodeid] = {
        'SUBMITTED': 0,
        'RETRIEVED': 0,
        'QUEUED': 0,
      }
    self.data[event][nodeid][tag] = v


  def addSyncHarvestSubmitted(self, event, item):
    self.addSyncHarvestRetrieved(event, item)


  def addSyncQueued(self, event, item):
    event = "synchronization status"
    tname = item.pop('threadName')
    tid = item.pop('threadId')
    message = item.pop('message')
    parts = message.split(':')
    v = int(parts[1].strip())
    self.setLastTimeStamp(item.pop('dateLogged'))
    nodeid = "TOTAL"
    try:
      nodeid = item.pop('nodeId')
    except:
      pass
    if nodeid not in self.data[event]:
      self.data[event][nodeid] = {
        'SUBMITTED': 0,
        'RETRIEVED': 0,
        'QUEUED': 0,
      }
    self.data[event][nodeid]['QUEUED'] = v


  def addReplicationStatus(self, event, item):
    event = "replication status"
    tname = item.pop('threadName')
    tid = item.pop('threadId')
    self.setLastTimeStamp(item.pop('dateLogged'))
    message = item.pop('message')
    parts = message.split(':')
    v = int(parts[1].strip())
    tag = parts[0].strip().split(" ")[2]
    item["count"] = v
    nodeid = item.pop('nodeId')
    if nodeid not in self.data[event]:
      self.data[event][nodeid] = {
          'COMPLETED': 0,
          'FAILED':0,
          'INVALIDATED':0,
          'QUEUED':0
        }
    self.data[event][nodeid][tag] = v


  def addEntry(self, entry):
    item = json.loads(entry)
    event = item.pop('event')
    if event in self.router:
      self.router[event](event, item)


  def __str__(self):
    return json.dumps(self.data, indent=2)

  def _get(self,a,b,c):
    if b == "TOTAL":
      if not (a == "synchronization status" and c == "QUEUED"):
        return " "
    try:
      return self.data[a][b][c]
    except:
      return 0


  def _getStatLabel(self, a, b, c):
    a = a.lower().strip()
    b = b.lower().strip()
    c = c.lower().strip()
    if a == "synchronization status":
      a = "synchron"
    else:
      a = "replicat"
    nodeid = b
    if not (nodeid == "total"):
      nodeid = b.split(":")[2]
    label = "{}.{}.{}.{}".format(self.environment, a, nodeid, c)
    return label


  def emitToStatsd(self, statsd_host, statsd_port=7125):
    client = statsd.StatsClient(statsd_host, statsd_port)
    stat = "synchronization status"
    for k in self.data[stat]:
      if k != "TOTAL":
        node = self.data[stat][k]
        for m in node:
          label = self._getStatLabel(stat, k, m)
          v = node[m]
          logging.debug("%s : %d", label, v)
          client.gauge(label, v)
    stat = "replication status"
    for k in self.data[stat]:
      if k != "TOTAL":
        node = self.data[stat][k]
        for m in node:
          label = self._getStatLabel(stat, k, m)
          v = node[m]
          logging.debug("%s : %d", label, v)
          client.gauge(label, v)


  def asText(self):
    res = ["Last Update: {}".format(self.data['dateLogged']),]
    res.append("{:>38} {:>22}".format("SYNCHRONIZATION", "REPLICATION"))
    res.append("{:20} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8}".format(
      "NodeID",
      "Queued",
      "Sub",
      "Retr",
      "Queued",
      "Comp",
      "Failed",
      "Invalid",
    ))
    nodes = []
    for k in self.data['synchronization status']:
      if k not in nodes:
        nodes.append(k)
    for k in self.data['replication status']:
      if k not in nodes:
        nodes.append(k)
    nodes.sort()
    for node in nodes:
      row = [
        node,
        self._get('synchronization status', node,'QUEUED'),
        self._get('synchronization status',node,'SUBMITTED'),
        self._get('synchronization status', node,'RETRIEVED'),
        self._get('replication status', node, 'QUEUED'),
        self._get('replication status', node, 'COMPLETED'),
        self._get('replication status', node, 'FAILED'),
        self._get('replication status', node, 'INVALIDATED'),
      ]
      res.append("{:20} {:8} {:8} {:8} {:8} {:8} {:8} {:8}".format(*row))
    res.append("")
    res.append("Synchronization")
    res.append("    Queued: Items in synchronization queue")
    res.append("       Sub: Items submitted to synchronization queue at last check")
    res.append("      Retr: Items retrieved from MN at last check")
    res.append("")
    res.append("Replication")
    res.append("    Queued: Items in replication queue")
    res.append("      Comp: Items reported as completed")
    res.append("    Failed: Items reported as failed")
    res.append("   Invalid: Items reported as invalid")
    res.append("")
    res.append("Note that metric values are only updated in response to an")
    res.append("event appearing in the cn-process-metric.log. There may")
    res.append("be significant latency in the reporting of some values.")
    res.append("")
    res.append("See also: /processing_metrics.json")
    res.append("")
    return("\n".join(res))


def main():
  parser = argparse.ArgumentParser(description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument('-l', '--log_level',
                      action='count',
                      default=0,
                      help='Set logging level, multiples for more detailed.')
  parser.add_argument('-m','--metrics_log',
                      default="/var/log/dataone/daemon/cn-process-metric.log",
                      help="Metrics log file to process.")
  parser.add_argument('-o','--offset_file',
                      default="/var/log/dataone/daemon/cn-process-metric.log.offset",
                      help="Offset file to record log reader position.")
  parser.add_argument('-j','--json_file',
                      default="/var/www/processing_metrics.json",
                      help="Metrics state file in JSON.")
  parser.add_argument('-t','--text_output',
                      default=None,
                      help="Text output file (stdout)")
  args = parser.parse_args()
  # Setup logging verbosity
  levels = [logging.WARNING, logging.INFO, logging.DEBUG]
  level = levels[min(len(levels) - 1, args.log_level)]
  logging.basicConfig(level=level,
                      format="%(asctime)s %(levelname)s %(message)s")

  collation = Collator(args.json_file)
  pyg = pygtail.Pygtail(args.metrics_log, offset_file=args.offset_file)
  for entry in pyg:
    collation.addEntry(entry)
  collation.save()
  fdest = sys.stdout
  if args.text_output is not None:
    fdest = open(args.text_output,"wt")
  fdest.write(collation.asText())
  collation.emitToStatsd("measure-unm-1.dataone.org")


if __name__ == "__main__":
  main()