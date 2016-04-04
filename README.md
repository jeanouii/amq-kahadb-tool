# amq-kahadb-tool

## KahaDBJournalsOptimizer

This tool allows reduce number journal files by moving durable subscriptions, pending messages and acks.

### Problems

[Why do KahaDB log files remain after cleanup?](http://activemq.apache.org/why-do-kahadb-log-files-remain-after-cleanup.html)

Clean-up of unreferenced KahaDB journal log files data-<id>.log will occur every 30 seconds by default. If a data file is in-use it will not be cleaned up.

A data file may be in-use because:
```sh
1. It contains a pending message for a destination or durable topic subscription.
2. It contains an ack for a message which is in an in-use data file - the ack cannot be removed 
   as a recovery would then mark the message for redelivery.
3. The journal references a pending transaction.
4. It is a journal file, and there may be a pending write to it.
```

### Decision

```sh
- 1 and 2 problems: Necessary moving durable subscriptions, pending messages and acks in one or more files.
- 3 problem: WARNING! Neglect transactions.
- 4 problem: Stop ActiveMQ.
```

### Use

```sh
1. Stop ActiveMQ.
2. Execute: java -jar .\out\artifacts\KahaDBJournalsOptimizer.jar "<journals directory>" 
   (example: java -jar .\out\artifacts\KahaDBJournalsOptimizer.jar "D:\Projects\apache-activemq-5.13.2\data\kahadb")
3. Start ActiveMQ.
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

This tool displays commands of journal files.

### Use

```sh
- Execute: java -jar .\out\artifacts\KahaDBJournalsReader.jar "<journals directory>" 
  (example: java -jar .\out\artifacts\KahaDBJournalsReader.jar "D:\Projects\apache-activemq-5.13.2\data\kahadb")
```

## KahaDBJournalsStatistics

This tool displays statistics journal files. Number of topics, queues and commands in the journal file.

### Use
```sh
- Execute: java -jar .\out\artifacts\KahaDBJournalsStatistics.jar "<journals directory>" 
  (example: java -jar .\out\artifacts\KahaDBJournalsStatistics.jar "D:\Projects\apache-activemq-5.13.2\data\kahadb")
```
