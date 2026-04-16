# Data store model

How data stores map inputs to outputs via unique IDs, how checkpointing works to skip already-processed items, the three backends (directory, zip, SQLite) and when to use each, and how citations and logs are stored alongside results.

## Data stores -- collections of data records

If you download [raw.zip](data/raw.zip) and unzip it, you will see it contains 1,035 files ending with a `.fa` filename suffix. (It also contains a tab delimited file and a log file, which we ignore for now.) The directory `raw` is a "data store" and the `.fa` files are "members" of it. In summary, a data store is a collection of members of the same "type". This means we can apply the same application to every member.

### Types of data store

| Class Name | Supported Operations | Supported Data Types | Identifying Suffix |
|---|---|---|---|
| `DataStoreDirectory` | read / write / append | text | None |
| `ReadOnlyDataStoreZipped` | read | text | `.zip` |
| `DataStoreSqlite` | read, write, append | text or bytes | `.sqlitedb` |

!!! note
    The `ReadOnlyDataStoreZipped` is just a compressed `DataStoreDirectory`.

### The structure of data stores

If a directory was not created by `scinexus` as a `DataStoreDirectory` then it has only the structure that existed previously.

If a data store was created by `scinexus`, either as a directory or as a `sqlitedb`, then it contains four types of data: completed records, *not* completed records, log files and md5 files. In a `DataStoreDirectory`, these are organised using the file system. The completed members are valid data records (as distinct from not completed) and are at the top level. The remaining types are in subdirectories.

```
demo_dstore
├── logs
├── md5
├── not_completed
└── ... <the completed members>
```

`logs/` stores `scitrack` log files produced by `scinexus` writer apps. `md5/` stores plain text files with the md5 sum of a corresponding data member which are used to check the integrity of the data store.

The `DataStoreSqlite` stores the same information, just in SQL tables.

[scitrack]: https://github.com/HuttleyLab/scitrack

