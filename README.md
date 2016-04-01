# amq-kahadb-tool

## KahaDBJournalsOptimizer

It tool allows reduce the number journals files by moving the durable subscription, pending messages and acks.

### Problems

[Why do KahaDB log files remain after cleanup?](http://activemq.apache.org/why-do-kahadb-log-files-remain-after-cleanup.html)

Clean-up of unreferenced KahaDB journal log files data-<id>.log will occur every 30 seconds by default. If a data file is in-use it will not be cleaned up.

A data file may be in-use because:
```sh
1. It contains a pending message for a destination or durable topic subscription.
2. It contains an ack for a message which is in an in-use data file - the ack cannot be removed as a recovery would then mark the message for redelivery.
3. The journal references a pending transaction.
4. It is a journal file, and there may be a pending write to it.
```

### Decision

```sh
- 1 and 2 problems: To reduce the number journals files is necessary moving the durable subscription, pending messages and acks in one or more files.
- 3 problem: WARNING! Transactions neglected.
- 4 problem: Stop ActiveMQ.
```

### Use

```sh
1. Stop ActiveMQ.
2. Run: java -jar .\out\artifacts\KahaDBJournalsOptimizer.jar "<journals directory>" (example: java -jar .\out\artifacts\KahaDBJournalsOptimizer.jar "D:\Projects\apache-activemq-5.13.2\data\kahadb")
3. Run ActiveMQ.
```

### Look

example:

```sh
java -jar .\out\artifacts\KahaDBJournalsOptimizer.jar "D:\Projects\apache-activemq-5.13.2\data\kahadb"
```
```sh
ANALYSIS
- Directory: "D:\Projects\apache-activemq-5.13.2\data\kahadb"
- Journals: 447 (Total size: 13.92 GB)
- Topics: 251
- Queues: 1
```
```sh
JOURNALS DATA OPTIMIZATION
- Topics subscriptions moved: 258
- Topics messages moved: 47
- Queues messages moved: 254

- Journals remained: 1 (Total size: 285.62 KB)
```

## KahaDBJournalsReader

It tool displays the commands in journals files.


### Use

```sh
- Run: java -jar .\out\artifacts\KahaDBJournalsReader.jar "<journals directory>" (example: java -jar .\out\artifacts\KahaDBJournalsReader.jar "D:\Projects\apache-activemq-5.13.2\data\kahadb")
```

## KahaDBJournalsStatistics

It tool displays the statistics journals files. Number of topics and queues in the file. The number of the commands and size in each topic and queue. The total size of all the commands in the file, etc.

### Use
```sh
- Run: java -jar .\out\artifacts\KahaDBJournalsStatistics.jar "<journals directory>" (example: java -jar .\out\artifacts\KahaDBJournalsStatistics.jar "D:\Projects\apache-activemq-5.13.2\data\kahadb")
```