# amq-kahadb-tool

## KahaDBJournalsOptimizer

> This tool allows reduce the number of journal files by moving durable subscriptions, pending messages and acks.

### The Problems

[Why do KahaDB log files remain after cleanup?](http://activemq.apache.org/why-do-kahadb-log-files-remain-after-cleanup.html)

Clean-up of unreferenced KahaDB journal log files data-<id>.log will occur every 30 seconds by default. If a data file is in-use it will not be cleaned up.

A data file may be in-use because:

```
   1. It contains a pending message for a destination or durable topic subscription.
   2. It contains an ack for a message which is in an in-use data file - the ack cannot be removed 
      as a recovery would then mark the message for redelivery.
   3. The journal references a pending transaction.
   4. It is a journal file, and there may be a pending write to it.
```

> If the files will not cleaned up, it can lead to the disk space to fill up.

### The Decisions

> Necessary moving all durable subscriptions, pending messages and part acks for them, saving pending transactions in one or more files.

### How to use

```
   1. Stop ActiveMQ.
   2. Execute: java -jar .\<release version>\KahaDBJournalsOptimizer.jar "journals directory"
   3. Start ActiveMQ.
```

### The Look

example:

```
   java -jar .\<release version>\KahaDBJournalsOptimizer.jar "D:\apache-activemq-5.13.2\data\kahadb"
```

result:
```
   -----------------------------------------------------------
   -----------------------------------------------------------
   START JOURNALS ANALYSIS

   - Directory: 'd:\activemq\apache-activemq-5.13.2\data\kahadb'.

   - Journal size: 32 MB (33554432).
   - Journals: 854 (Total size: 26,69 GB (28655341738)).
   - Topics: 1248.
   - Queues: 2.

   - It took time: 423.931 seconds.
   -----------------------------------------------------------
   START JOURNALS DATA OPTIMIZATION

   - Subscriptions moved: 1285 (It took time: 75.447 seconds).
   - Messages moved: 5378 (It took time: 259.997 seconds).
   - Journals remained: 1 (Total size: 3,48 MB (3651054)).

   - It took time: 335.494 seconds.
   -----------------------------------------------------------
   -----------------------------------------------------------
   RENAME JOURNALS DIRECTORIES

   - Directory optimized: 'D:\activemq\apache-activemq-5.13.2\data\kahadb'.
   - Backup directory: 'D:\activemq\apache-activemq-5.13.2\data\kahadb_1459951397015'.

   -----------------------------------------------------------
   -----------------------------------------------------------
```

## KahaDBJournalsReader

> This tool displays commands of journal files.

### How to use

```
   java -jar .\<release version>\KahaDBJournalsReader.jar "journals directory"
```

### The Look

example:

```
   java -jar .\<release version>\KahaDBJournalsReader.jar "D:\apache-activemq-5.13.2\data\kahadb"
```

result:

```
   -----------------------------------------------------------
   (1) Journal: 'd:\activemq\apache-activemq-5.13.2\data\kahadb\db-1.log'.

   1 CommandType: KAHA_SUBSCRIPTION_COMMAND - TOPIC (DestId: 1:test.topic.0), ClientId: client0, SubKey: client0:EXACTLY_ONCE:test/topic/0.
   2 CommandType: KAHA_SUBSCRIPTION_COMMAND - TOPIC (DestId: 1:test.topic.1), ClientId: client1, SubKey: client1:EXACTLY_ONCE:test/topic/1.
   3 CommandType: KAHA_SUBSCRIPTION_COMMAND - TOPIC (DestId: 1:test.topic.2), ClientId: client2, SubKey: client2:EXACTLY_ONCE:test/topic/2.
   4 CommandType: KAHA_ADD_MESSAGE_COMMAND - TOPIC (DestId: 1:test.topic.2), MsgId: ID:compPC-41418-1459948891354-1:1:1:1:1, TransId: TX:ID:compPC-41418-1459948891354-1:1:1.
   5 CommandType: KAHA_ADD_MESSAGE_COMMAND - TOPIC (DestId: 1:test.topic.2), MsgId: ID:compPC-41418-1459948891354-1:1:1:1:2, TransId: TX:ID:compPC-41418-1459948891354-1:1:1.
   6 CommandType: KAHA_ADD_MESSAGE_COMMAND - TOPIC (DestId: 1:test.topic.2), MsgId: ID:compPC-41418-1459948891354-1:1:1:1:3, TransId: TX:ID:compPC-41418-1459948891354-1:1:1.
   7 CommandType: KAHA_ADD_MESSAGE_COMMAND - TOPIC (DestId: 1:test.topic.2), MsgId: ID:compPC-41418-1459948891354-1:1:1:1:4, TransId: TX:ID:compPC-41418-1459948891354-1:1:1.
   8 CommandType: KAHA_ADD_MESSAGE_COMMAND - TOPIC (DestId: 1:test.topic.2), MsgId: ID:compPC-41418-1459948891354-1:1:1:1:5, TransId: TX:ID:compPC-41418-1459948891354-1:1:1.
   9 CommandType: KAHA_COMMIT_COMMAND - TransId: TX:ID:compPC-41418-1459948891354-1:1:1.
   -----------------------------------------------------------
```

## KahaDBJournalsStatistics

> This tool displays statistics journal files. Number of topics, queues and commands in the journal file.

### How to use

```
   java -jar .\<release version>\KahaDBJournalsStatistics.jar "journals directory"
```

### The Look

example:

```
   java -jar .\<release version>\KahaDBJournalsStatistics.jar "D:\apache-activemq-5.13.2\data\kahadb"
```

result:

```
   -----------------------------------------------------------
   -----------------------------------------------------------
   (1) Journal: 'd:\activemq\apache-activemq-5.13.2\data\kahadb\db-1.log'.
   -----------------------------------------------------------
   Destination statistics:
   - Topics: 3.
   - Queues: 0.

   TOPIC (DestId: 1:test.topic.0).
   + CmdType: KAHA_SUBSCRIPTION_COMMAND (Count: 1, TotalSize: 172 Byte(s) (172), ~AvrgSize: 172 Byte(s) (172), LastBigSize: 172 Byte(s) (172), LastSize: 172 Byte(s) (172))
   All commands: 1 (Total size: 172 Byte(s) (172).

   TOPIC (DestId: 1:test.topic.1).
   + CmdType: KAHA_SUBSCRIPTION_COMMAND (Count: 1, TotalSize: 172 Byte(s) (172), ~AvrgSize: 172 Byte(s) (172), LastBigSize: 172 Byte(s) (172), LastSize: 172 Byte(s) (172))
   All commands: 1 (Total size: 172 Byte(s) (172).

   TOPIC (DestId: 1:test.topic.2).
   + CmdType: KAHA_SUBSCRIPTION_COMMAND (Count: 1, TotalSize: 172 Byte(s) (172), ~AvrgSize: 172 Byte(s) (172), LastBigSize: 172 Byte(s) (172), LastSize: 172 Byte(s) (172))
   + CmdType: KAHA_ADD_MESSAGE_COMMAND (Count: 5, TotalSize: 2,18 KB (2235), ~AvrgSize: 447 Byte(s) (447), LastBigSize: 447 Byte(s) (447), LastSize: 447 Byte(s) (447))
   All commands: 6 (Total size: 2,35 KB (2407).

   Commands without destination:
   + CmdType: KAHA_COMMIT_COMMAND (Count: 1, TotalSize: 79 Byte(s) (79), ~AvrgSize: 79 Byte(s) (79), LastBigSize: 79 Byte(s) (79), LastSize: 79 Byte(s) (79))
   All commands: 1 (Total size: 79 Byte(s) (79).
   -----------------------------------------------------------
   Command statistics:
   - Topics: 3 (messages: 8, +subscriptions: 3, -subscription: 0).
   - Queues: 0 (messages: 0).
   - Other messages: 1.

   Commands:
   + CmdType: KAHA_SUBSCRIPTION_COMMAND (Count: 3, TotalSize: 516 Byte(s) (516), ~AvrgSize: 172 Byte(s) (172), LastBigSize: 172 Byte(s) (172), LastSize: 172 Byte(s) (172))
   + CmdType: KAHA_ADD_MESSAGE_COMMAND (Count: 5, TotalSize: 2,18 KB (2235), ~AvrgSize: 447 Byte(s) (447), LastBigSize: 447 Byte(s) (447), LastSize: 447 Byte(s) (447))
   + CmdType: KAHA_COMMIT_COMMAND (Count: 1, TotalSize: 79 Byte(s) (79), ~AvrgSize: 79 Byte(s) (79), LastBigSize: 79 Byte(s) (79), LastSize: 79 Byte(s) (79))
   All commands: 9 (Total size: 2,76 KB (2830).
   -----------------------------------------------------------
   -----------------------------------------------------------
```