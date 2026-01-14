# GoDB-v2

> **⚠️ WORK IN PROGRESS**

## Overview

**GoDB** is a disk-based relational database management system written in Go. It implements a classic DBMS architecture featuring a paged storage engine, a buffer pool manager, a Volcano-style execution engine, and a transaction system providing ACID guarantees via Write-Ahead Logging (WAL) and ARIES-style recovery.

## Architecture

GoDB follows a layered architecture:

1. **Disk Storage & Buffer Pool**: Manages physical files, pages (4KB fixed size), and caching.
2. **Access Methods**: Heap files for table storage and in-memory B+Trees/Hash maps for indexing.
3. **Transactional Core**: Implements tuple-level locking (2PL) and write-ahead logging for concurrency control and recovery.
4. **Execution Engine**: A pipeline (iterator-based) execution engine supporting filtering, aggregations, and joins.
5. **SQL Frontend**: Support for a simple subset of SQL + a simple rule-based optimizer (planned)

## Key Features

### Storage Engine

* Fixed-size pages using a **Heap File** organization. Since GoDB supports only integers and 32-byte strings, pages use a bitmap to track slots.
* Deletes marked with tombstone in a separate bitmap per page
* Free-space management with Vacuums + metadata page tracking occupancy levels
* Buffer Pool with fine-grained locking + CLOCK

### Indexing

* **Interfaces**: Generic Index interface supporting Point Lookups and Range Scans. For simplicity, currently both indexes are provided and in-memory only.
* **B+Tree**: In-memory implementation wrapper (using `tidwall/btree`).
* **Hash Index**: In-memory concurrent hash map.

### Execution Engine

* Assumes working set fits in-memory for simplicity, projections are not pushed into physical operators.
* **Basics**: Filter, Projection, Limit
* **Scans**: Sequential Scan, Index Scan, and Index-Only Scan.
* **Joins**: Hash Join, Sort-Merge Join, Nested Loop Join (Block + Index-based)
* **Aggregations**: Hash-based aggregation (Count, Sum, Min, Max).
* **Sorting**: In-memory sort operator, top-n with in-memory heap
* **Modifications**: Insert, Update, Delete operators.
* **Other**: Materialize operator to buffer results

### Transactions & Concurrency

* **Locking**: Strict Two-Phase Locking (S2PL) achieving READ COMMITTED.
* **Deadlock Prevention**: Implements the **Wait-Die** scheme for deadlock handling.
* **Isolation**: Supports tuple-level and table-level locking modes (S, X, IS, IX, SIX).

### Recovery (ARIES)

* **Write-Ahead Logging (WAL)**: Ring-buffered, concurrent logging system.
* **Crash Recovery**: Simplified ARIES implementation -- undo records are buffered in-memory to avoid a backward scan in transaction abort and in recovery
* **Checkpoints**: Fuzzy checkpoints.
* **Indexes**: Since indexes are in-memory only, they do not participate in logging or recovery. Indexes are rebuilt on recovery. Index updates are undone on transaction abort using in-memory callbacks


## TODOs
* Infrastructure: fix build scripts and review handout code
* Lab 2: execution
* Lab 3: transactions
* Lab 4: logging and recovery
* Enhancement: SQL frontend
* Enhancement: Rule-based optimizer
* Enhancement: leaderboard for extra-credit tasks
