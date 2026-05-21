# JavaDB

JavaDB is a relational database engine written in Java. It stores data in a paged, disk-backed file format and provides ACID transactions over a multi-user workload, with cost-based query optimization and crash recovery.

## Features

### Storage and indexing
- Slotted-page heap files backed by a buffer pool with configurable eviction policies (LRU, clock)
- B+ tree index implementation supporting point lookups, range scans, and bulk loading
- Split and merge logic that keeps the tree balanced under inserts and deletes

### Query execution
- SQL parser that produces a relational algebra tree
- Pull-based iterator model (Volcano-style) for query execution
- Join operators: block nested loop, sort-merge, and grace hash join
- External sorting for inputs that do not fit in memory

### Query optimization
- System R-style cost-based optimizer
- Dynamic programming enumeration of left-deep join plans
- Table statistics and histogram-based selectivity estimates
- Selection and projection pushdown

### Concurrency
- Multigranularity locking protocol with intent locks at the database, table, and page levels
- Lock escalation to reduce lock manager overhead under heavy contention
- A lock manager that arbitrates between concurrent transactions to guarantee serializability

### Crash recovery
- ARIES-style recovery manager with write-ahead logging
- Fuzzy checkpointing so checkpoints do not block running transactions
- Three-pass recovery (analysis, redo, undo) that restores the database to a transaction-consistent state after arbitrary crashes, including crashes that occur during recovery itself

## Project layout

```
src/main/java/edu/berkeley/cs186/database/
├── Database.java           Top-level entry point
├── Transaction.java        User-facing transaction API
├── cli/                    SQL parser and interactive shell
├── common/                 Shared utilities
├── concurrency/            Lock manager and multigranularity locking
├── databox/                Typed value boxes (int, string, etc.)
├── index/                  B+ tree implementation
├── io/                     Disk and page management
├── memory/                 Buffer pool and eviction policies
├── query/                  Operators, optimizer, statistics
├── recovery/               ARIES recovery manager
└── table/                  Heap files, records, schemas
```

## Building and running

The project is built with Maven and targets Java 8.

Compile and run tests:

```sh
mvn clean test
```

Launch the interactive SQL shell:

```sh
mvn clean compile
java -cp target/classes edu.berkeley.cs186.database.cli.CommandLineInterface
```

## Requirements

- Java 8 or later
- Maven 3.x
