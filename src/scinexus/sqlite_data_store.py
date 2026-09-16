from __future__ import annotations

import datetime
import os
import re
import sqlite3
import threading
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scitrack import get_text_hexdigest  # type: ignore[import-untyped]

from scinexus.data_store import (
    APPEND,
    LOG_TABLE,
    OVERWRITE,
    READONLY,
    DataMember,
    DataMemberABC,
    DataStoreABC,
    DataStoreDirectory,
    Mode,
    _check_identifier,
)
from scinexus.misc import extend_docstring_from
from scinexus.parallel import is_master_process

if TYPE_CHECKING:  # pragma: no cover
    from citeable import CitationBase

RESULT_TABLE = "results"
_MEMORY = ":memory:"
_mem_pattern = re.compile(r"^\s*[:]{0,1}memory[:]{0,1}\s*$")
NoneType = type(None)

# dealing with python3.12 deprecation of datetime objects and their sqlite3 handling


def _datetime_to_iso(timestamp: datetime.datetime) -> str:
    """timestamp in ISO 8601 format"""
    return timestamp.isoformat()


sqlite3.register_adapter(datetime.datetime, _datetime_to_iso)


def _datetime_from_iso(data: bytes) -> datetime.datetime:
    """timestamp from ISO 8601 format"""
    return datetime.datetime.fromisoformat(data.decode())


sqlite3.register_converter("timestamp", _datetime_from_iso)


# create db
def open_sqlite_db_rw(path: str | Path) -> sqlite3.Connection:
    """creates a new sqlitedb for read/write at path, can be an in-memory db

    The connection may be used from any thread, so a caller that shares one
    between threads must serialise its use.

    Notes
    -----
    This function embeds the schema. There are three tables:

    - results: analysis objects, may be completed or not completed
    - logs: log-file contents
    - state: whether db is locked to a process

    Returns
    -------
    Handle to a sqlite3 session
    """
    db = sqlite3.connect(
        path,
        isolation_level=None,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        check_same_thread=False,
    )
    db.row_factory = sqlite3.Row
    create_template = "CREATE TABLE IF NOT EXISTS {};"
    # note it is essential to use INTEGER for the autoincrement of primary key to work
    creates = [
        "state(state_id INTEGER PRIMARY KEY, record_type TEXT, lock_pid INTEGER)",
        f"{LOG_TABLE}(log_id INTEGER PRIMARY KEY, log_name TEXT, date timestamp, data BLOB)",
        f"{RESULT_TABLE}(record_id TEXT PRIMARY KEY, log_id INTEGER, md5 BLOB, is_completed INTEGER, data BLOB)",
        "citations(citation_id INTEGER PRIMARY KEY, data TEXT)",
    ]
    for table in creates:
        db.execute(create_template.format(table))
    return db


def open_sqlite_db_ro(path: str | Path) -> sqlite3.Connection:
    """returns db opened as read only

    The connection may be used from any thread, so a caller that shares one
    between threads must serialise its use.

    Returns
    -------
    Handle to a sqlite3 session
    """
    db = sqlite3.connect(
        f"file:{path}?mode=ro",
        isolation_level=None,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        uri=True,
        check_same_thread=False,
    )
    db.row_factory = sqlite3.Row
    if not has_valid_schema(db):
        msg = "database does not have a valid schema"
        raise ValueError(msg)
    return db


def _owner_token() -> str:
    """identifies the process and thread taking a lock

    Notes
    -----
    Written into ``state.lock_pid``, whose INTEGER is an affinity rather
    than a constraint, so the column takes this without a schema change.
    A value that is still a bare integer was written by a version that
    recorded only the process.
    """
    return f"{os.getpid()}:{threading.get_native_id()}"


def has_valid_schema(db: sqlite3.Connection) -> bool:
    # TODO: should be a full schema check
    query = "SELECT name FROM sqlite_master WHERE type='table'"
    result = db.execute(query).fetchall()
    table_names = {r["name"] for r in result}
    _required = {RESULT_TABLE, LOG_TABLE, "state"}
    _optional = {"citations"}
    return _required <= table_names <= (_required | _optional)


class DataStoreSqlite(DataStoreABC):
    """data store backed by a SQLite database

    A store is opened and written by one session and read from any thread.
    It runs its statements one at a time on a single connection, so
    concurrent readers get their own rows back. A caller that takes the
    connection from :attr:`db` and uses it itself is outside that.
    """

    store_suffix = "sqlitedb"

    def __init__(
        self,
        source: str | Path,
        mode: Mode | str = READONLY,
        limit: int | None = None,
        verbose: bool = False,
    ) -> None:
        if _mem_pattern.search(str(source)):
            self._source: str | Path = _MEMORY
        else:
            source = Path(source).expanduser()
            self._source = (
                source
                if source.suffix[1:] == self.store_suffix  # sliced to remove "."
                else Path(f"{source}.{self.store_suffix}")
            )
        self._mode = Mode(mode)
        if mode is not READONLY and limit is not None:
            msg = "Using limit argument is only valid for readonly datastores"
            raise ValueError(
                msg,
            )
        self._limit = limit
        self._verbose = verbose
        self._holds_lock = False
        self._db: sqlite3.Connection | None = None
        self._closed = False
        self._log_id: int | None = None
        self._opened_by = _owner_token()
        if self._mode is not READONLY and is_master_process():
            # opening a store for writing is not writing to it: it may be
            # opened to read, or to release a lock left behind. so a lock
            # this cannot take is left for the write that meets it to raise
            self._open_and_claim(warn=False)

    def __getstate__(self) -> dict[str, object]:
        return {**self._init_vals}

    def __setstate__(self, state: dict[str, Any]) -> None:
        # this will reset connections to read only db's
        obj = self.__class__(**state)
        self.__dict__.update(obj.__dict__)

    def __del__(self) -> None:
        """drop the connection, leaving the lock to mark an unclosed store"""
        # no SQL: this runs during collection and at interpreter exit, where
        # a statement can block on another connection's write lock or find
        # the machinery it needs already torn down
        db: sqlite3.Connection | None = getattr(self, "_db", None)
        if db is None:
            return
        db.close()
        # the warning is about a lock left on disk for the next opener to
        # trip over, so it is worth making only by a store that took one and
        # wrote it somewhere that outlives the process. closing first means
        # a warning turned into an error cannot strand the connection
        if getattr(self, "_holds_lock", False) and self._source != _MEMORY:
            warnings.warn(
                f"data store {str(self.source)!r} was not closed, so it still "
                "holds the lock. call close() to release it",
                UserWarning,
                stacklevel=1,
            )

    @property
    def source(self) -> str | Path:
        """string that references connecting to data store, override in subclass constructor"""
        return self._source

    @property
    def mode(self) -> Mode:
        """string that references datastore mode, override in override in subclass constructor"""
        return self._mode

    @property
    def limit(self) -> int | None:
        return self._limit

    def _check_open(self) -> None:
        """raise if the store has been closed"""
        if self._closed:
            msg = f"data store {str(self.source)!r} is closed"
            raise OSError(msg)

    def _open_and_claim(self, warn: bool = True) -> None:
        """open the database and take its lock for this session

        A lock another session holds leaves a store that reads but refuses
        every write until ``unlock(force=True)`` releases it. Reported as a
        warning unless *warn* is false.
        """
        try:
            _ = self.db
        except OSError as refused:
            # a lock is the only OSError reachable from here: the store
            # cannot yet be closed, and sqlite's own failures are not
            # OSError. raising would make the message's own advice
            # impossible to follow, since it is this store that has to
            # carry out the release
            if warn:
                warnings.warn(str(refused), UserWarning, stacklevel=2)

    def _check_writing_session(self) -> None:
        """raise unless this is the session that opened the store

        Reading is open to any thread.
        """
        # a write from elsewhere lands on a database this session holds the
        # lock on, under a log entry it did not open
        if _owner_token() == self._opened_by:
            return

        msg = (
            f"data store {str(self.source)!r} was opened by {self._opened_by}, "
            f"which is the only session that may write to it. This is "
            f"{_owner_token()}."
        )
        raise OSError(msg)

    def _connection(self) -> sqlite3.Connection:
        """the connection, opened if it is not already, taking no lock"""
        # the open is under the same lock as every use of what it returns.
        # threads racing this check-then-set each open a database only one
        # of them goes on to use, and every other is orphaned: a descriptor
        # and a page cache nothing will close
        with self._cache_lock:
            self._check_open()
            if self._db is None:
                db_func = (
                    open_sqlite_db_ro if self.mode is READONLY else open_sqlite_db_rw
                )
                self._db = db_func(self.source)

            if self._db is None:
                msg = "database connection is unexpectedly None"
                raise ValueError(msg)
            return self._db

    @property
    def db(self) -> sqlite3.Connection:
        with self._cache_lock:
            db = self._connection()
            # taking the lock is a separate step from opening, and is
            # retried until it succeeds. a refusal that left a connection
            # behind would be taken as proof of a lock by every access
            # after it.

            # ownership is thread scoped, so a worker reading a store whose
            # lock is free must not take one here: the opening session
            # could then neither write behind it nor release it
            if not self._holds_lock and _owner_token() == self._opened_by:
                self.lock()
            return db

    def _db_for(self, *, claim: bool) -> sqlite3.Connection:
        """the connection, claiming the store's lock unless told not to

        ``claim=False`` reaches a store this session may not write to.
        """
        return self.db if claim else self._connection()

    def _execute(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        claim: bool = True,
    ) -> None:
        """run a statement on the store's connection"""
        # a connection carries a statement cache that CPython keys by the
        # text of the statement and shares between threads without
        # protection, so two threads running one SELECT with different
        # parameters bind the same statement and read each other's rows.
        # every statement here and in _claim is taken under this lock
        with self._cache_lock:
            self._db_for(claim=claim).execute(sql, params)

    def _fetchone(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        claim: bool = True,
    ) -> Any:  # noqa: ANN401
        """the first row of a statement, or None if it produced none"""
        with self._cache_lock:
            cursor = self._db_for(claim=claim).execute(sql, params)
            try:
                return cursor.fetchone()
            finally:
                # a statement left part way through a scan is still checked
                # out of the cache, which is the state another thread trips
                # over, so the cursor goes before the lock does
                cursor.close()

    def _fetchall(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
        *,
        claim: bool = True,
    ) -> list[Any]:
        """every row of a statement"""
        with self._cache_lock:
            cursor = self._db_for(claim=claim).execute(sql, params)
            try:
                return cursor.fetchall()
            finally:
                cursor.close()

    def _init_log(self) -> None:
        timestamp = datetime.datetime.now(tz=datetime.UTC)
        self._execute(f"INSERT INTO {LOG_TABLE}(date) VALUES (?)", (timestamp,))
        self._log_id = self._fetchone(
            f"SELECT log_id FROM {LOG_TABLE} where date = ?",
            (timestamp,),
        )["log_id"]

    def close(self) -> None:
        """release the lock and the connection, ending the store's life

        Waits for any statement another thread has in flight. Warns if the
        lock is one this session may not release, since the store is going
        and nothing after it will report the lock left behind.
        """
        with self._cache_lock:
            db: sqlite3.Connection | None = getattr(self, "_db", None)
            if db is None:
                self._closed = True
                return
            try:
                # an explicit close is the only thing that releases the
                # lock, so one still set marks a store whose session ended
                # another way
                self.unlock()
                # a lock taken by another thread is not this one's to
                # release, and the store is going, so nothing else will
                # report it
                held = self._lock_id if self._holds_lock else None
                if held is not None and self._source != _MEMORY:
                    warnings.warn(
                        f"data store {str(self.source)!r} is still locked by "
                        f"{held}, call unlock(force=True) before closing to clear it",
                        UserWarning,
                        stacklevel=2,
                    )
            finally:
                # in a finally so a store whose lock could not be released
                # is still shut, rather than left usable by the failure
                self._db = None
                self._closed = True
                # all three describe the connection that is going
                self._log_id = None
                self._completed = []
                self._not_completed = []
                db.close()

    def read(self, unique_id: str) -> str | bytes:
        """
        identifier string formed from Path(table_name) / identifier
        """
        uid_path = Path(unique_id)
        table_name = str(uid_path.parent)
        if table_name not in (
            ".",
            LOG_TABLE,
        ):
            msg = f"unknown table for {str(uid_path)!r}"
            raise ValueError(msg)

        if table_name != LOG_TABLE:
            cmnd = f"SELECT * FROM {RESULT_TABLE} WHERE record_id = ?"
            result = self._fetchone(cmnd, (uid_path.name,))
            return result["data"]

        cmnd = f"SELECT * FROM {LOG_TABLE} WHERE log_name = ?"
        result = self._fetchone(cmnd, (uid_path.name,))

        return result["data"]

    @property
    def completed(self) -> list[DataMemberABC]:
        # the lock spans check, scan and publish, so concurrent readers make
        # one scan between them rather than one each
        with self._cache_lock:
            if not self._completed:
                self._completed = self._select_members(
                    table_name=RESULT_TABLE,
                    is_completed=True,
                )
            return self._completed

    @property
    def not_completed(self) -> list[DataMemberABC]:
        """returns database records of type NotCompleted"""
        with self._cache_lock:
            if not self._not_completed:
                self._not_completed = self._select_members(
                    table_name=RESULT_TABLE,
                    is_completed=False,
                )
            return self._not_completed

    def _select_members(
        self,
        *,
        table_name: str,
        is_completed: bool,
    ) -> list[DataMemberABC]:
        limit = f"LIMIT {self.limit}" if self.limit else ""
        rows = self._fetchall(
            f"SELECT record_id FROM {table_name} WHERE is_completed=? {limit}",
            (is_completed,),
        )
        return [DataMember(data_store=self, unique_id=r["record_id"]) for r in rows]

    @property
    def logs(self) -> list[DataMemberABC]:
        """returns all log records"""
        rows = self._fetchall(f"SELECT log_name FROM {LOG_TABLE}")
        return [
            DataMember(data_store=self, unique_id=Path(LOG_TABLE) / r["log_name"])
            for r in rows
            if r["log_name"]
        ]

    def _write(
        self,
        *,
        table_name: str,
        unique_id: str,
        data: str | bytes,
        is_completed: bool,
    ) -> DataMemberABC | None:
        """
        Parameters
        ----------
        table_name
            name of table to save data. It must be _RESULT_TABLE or _LOG_TABLE.
        unique_id
            unique identifier that data will be saved under.
        data
            data to be saved.
        is_completed
            flag to identify NotCompleted results

        Returns
        -------
        DataMember instance or None when writing to _LOG_TABLE

        Raises
        ------
        ValueError
            if unique_id does not name a record
        OSError
            if called from a session other than the one that opened the
            store
        """
        self._check_writing_session()
        _check_identifier(unique_id)
        if self._log_id is None:
            self._init_log()

        if table_name == LOG_TABLE:
            # TODO how to evaluate whether writing a new log?
            cmnd = f"UPDATE {table_name} SET data =?, log_name =? WHERE log_id=?"
            self._execute(cmnd, (data, unique_id, self._log_id))
            return None

        md5 = get_text_hexdigest(data)

        if unique_id in self and self.mode is not APPEND:
            cmnd = f"UPDATE {table_name} SET data= ?, log_id=?, md5=? WHERE record_id=?"
            self._execute(cmnd, (data, self._log_id, md5, unique_id))
        else:
            cmnd = f"INSERT INTO {table_name} (record_id,data,log_id,md5,is_completed) VALUES (?,?,?,?,?)"
            self._execute(cmnd, (unique_id, data, self._log_id, md5, is_completed))

        return DataMember(data_store=self, unique_id=unique_id)

    def drop_not_completed(self, *, unique_id: str | None = None) -> None:
        """remove not-completed records from the database

        Parameters
        ----------
        unique_id
            if provided, only drop the record with this identifier,
            otherwise drop all not-completed records

        Raises
        ------
        OSError
            if called from a session other than the one that opened the
            store
        """
        self._check_writing_session()
        vals: tuple[int] | tuple[int, str]
        if not unique_id:
            cmnd = f"DELETE FROM {RESULT_TABLE} WHERE is_completed=?"
            vals = (0,)
        else:
            cmnd = f"DELETE FROM {RESULT_TABLE} WHERE is_completed=? AND record_id=?"
            vals = (0, unique_id)
        self._execute(cmnd, vals)
        self._not_completed = []

    @property
    def _lock_id(self) -> int | str | None:
        """returns lock_pid: an owner token, or a bare pid from an older version"""
        result = self._fetchone(
            "SELECT lock_pid FROM state WHERE lock_pid IS NOT NULL "
            "ORDER BY state_id LIMIT 1",
            claim=False,
        )
        return result[0] if result else None

    @property
    def locked(self) -> bool:
        """returns if the store is locked at all"""
        return self._lock_id is not None

    def lock(self) -> None:
        """if writable, and not locked, locks the database to this session"""
        with self._cache_lock:
            self._check_open()
            if self.mode is READONLY:
                return
            if self._db is None:
                msg = "database connection is unexpectedly None"
                raise RuntimeError(msg)
            # a store already locked is answered from this read, which needs
            # only a shared lock. claiming takes the write lock, so going
            # straight there would make a refusal wait out the busy timeout
            # behind any writer and then fail as OperationalError
            locked = self._lock_id
            if locked is None:
                locked = self._claim()

            # a lock marks a store whose session did not end through
            # close(), so its records were never confirmed complete. no
            # mode that writes may build on that without being told to
            if locked is not None:
                msg = (
                    f"You are trying to open {str(self.source)!r} for writing but "
                    f"it is locked by {locked}. Call unlock(force=True) on a "
                    "writable store to release it."
                )
                raise OSError(
                    msg,
                )
            self._holds_lock = True

    def _claim(self) -> int | str | None:
        """record this session as the owner, or report who already is"""
        # the whole transaction is one region rather than a statement at a
        # time: a thread reading between the BEGIN and the COMMIT reads
        # inside a transaction it knows nothing of. so a BEGIN waiting out
        # the busy timeout behind a competing process holds up every reader
        # of this store, not just this one
        with self._cache_lock:
            db = self._connection()
            db.execute("BEGIN IMMEDIATE")
            try:
                result = db.execute(
                    "SELECT state_id,lock_pid FROM state ORDER BY state_id",
                ).fetchall()
                # is not None rather than truthy: a lock recorded as 0 is a lock
                locked = next(
                    (r["lock_pid"] for r in result if r["lock_pid"] is not None),
                    None,
                )
                if locked is None and result:
                    # we will update an existing
                    state_id = result[0]["state_id"]
                    db.execute(
                        "UPDATE state SET lock_pid=? WHERE state_id=?",
                        (_owner_token(), state_id),
                    )
                    if len(result) > 1:
                        # rows the two-statement version left behind. reaching
                        # here means none of them holds a lock, and only the
                        # first carries a record_type, so they record nothing
                        db.execute("DELETE FROM state WHERE state_id>?", (state_id,))
                elif locked is None:
                    db.execute(
                        "INSERT INTO state(lock_pid) VALUES (?)",
                        (_owner_token(),),
                    )
                # inside the try: a commit wants an exclusive lock where the
                # begin wanted a reserved one, so it is the statement here most
                # likely to be refused, and one left unended strands the
                # connection in a transaction it can never start another after
                db.execute("COMMIT")
            except BaseException:
                # sqlite ends the transaction itself on some errors, and asking
                # again then raises over the real failure
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise
            return locked

    def unlock(self, force: bool = False) -> None:
        """remove a lock this session took. If force, remove any. ignored if mode is READONLY"""
        with self._cache_lock:
            self._check_open()
            if self.mode is READONLY:
                return

            lock_id = self._lock_id
            if lock_id is None:
                return

            # a lock recorded by an older version names only a process,
            # which this session cannot claim to be, so clearing one takes
            # force
            if lock_id == _owner_token() or force:
                self._execute("UPDATE state SET lock_pid=NULL", claim=False)
                self._holds_lock = False

    @extend_docstring_from(DataStoreDirectory.write)
    def write(self, *, unique_id: str, data: str | bytes) -> DataMemberABC:  # type: ignore[override]
        if unique_id.startswith(RESULT_TABLE):
            unique_id = Path(unique_id).name

        super().write(unique_id=unique_id, data=data)

        self.drop_not_completed(unique_id=unique_id)

        member = self._write(
            table_name=RESULT_TABLE,
            unique_id=unique_id,
            data=data,
            is_completed=True,
        )
        if member is None:
            msg = "write to results table failed to produce a member"
            raise RuntimeError(msg)
        if member not in self._completed:
            self._completed.append(member)
        return member

    @extend_docstring_from(DataStoreDirectory.write_log)
    def write_log(self, *, unique_id: str, data: str | bytes) -> None:
        if unique_id.startswith(LOG_TABLE):
            unique_id = Path(unique_id).name

        super().write_log(unique_id=unique_id, data=data)
        _ = self._write(
            table_name=LOG_TABLE,
            unique_id=unique_id,
            data=data,
            is_completed=False,
        )

    @extend_docstring_from(DataStoreDirectory.write_not_completed)
    def write_not_completed(  # type: ignore[override]
        self, *, unique_id: str, data: str | bytes
    ) -> DataMemberABC:
        if unique_id.startswith(RESULT_TABLE):
            unique_id = Path(unique_id).name

        super().write_not_completed(unique_id=unique_id, data=data)
        member = self._write(
            table_name=RESULT_TABLE,
            unique_id=unique_id,
            data=data,
            is_completed=False,
        )
        if member is None:
            msg = "write to results table failed to produce a member"
            raise RuntimeError(msg)
        self._not_completed.append(member)
        return member

    def md5(self, unique_id: str) -> str | None:
        """
        Parameters
        ----------
        unique_id
            name of data store member
        Returns
        -------
        md5 checksum for the member, if available, None otherwise
        """
        cmnd = f"SELECT * FROM {RESULT_TABLE} WHERE record_id = ?"
        result = self._fetchone(cmnd, (unique_id,))

        return result["md5"] if result else None

    def write_citations(self, *, data: tuple[CitationBase, ...]) -> None:
        if not data:
            return
        self._check_writing_session()
        if not self._has_citations_table():
            self._execute(
                "CREATE TABLE IF NOT EXISTS citations"
                "(citation_id INTEGER PRIMARY KEY, data TEXT)",
            )
        from citeable import to_jsons

        json_data = to_jsons(data)
        if existing := self._fetchone("SELECT citation_id FROM citations"):
            self._execute(
                "UPDATE citations SET data=? WHERE citation_id=?",
                (json_data, existing["citation_id"]),
            )
        else:
            self._execute("INSERT INTO citations(data) VALUES (?)", (json_data,))

    def _load_citations(self) -> list[CitationBase]:
        from citeable import from_jsons

        if not self._has_citations_table():
            return []
        result = self._fetchone("SELECT data FROM citations")
        return from_jsons(result["data"]) if result else []

    def _has_citations_table(self) -> bool:
        result = self._fetchone(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='citations'",
        )
        return result is not None

    def _describe(self) -> dict[str, object]:
        if self.locked and self._lock_id != _owner_token():
            title = (
                f"Locked db store. Locked by {self._lock_id}, "
                f"this session is {_owner_token()}."
            )
        elif self.locked:
            title = "Locked to the current process and thread."
        else:
            title = "Unlocked db store."
        result = super()._describe()
        result["title"] = title
        return result

    @property
    def record_type(self) -> str:
        """class name of completed results"""
        result = self._fetchone("SELECT record_type FROM state")
        return result["record_type"]

    @record_type.setter
    def record_type(self, obj: object) -> None:
        from scinexus.misc import get_object_provenance

        self._check_writing_session()
        rt = self.record_type
        if self.mode is OVERWRITE and rt:
            msg = f"cannot overwrite existing record_type {rt}"
            raise OSError(msg)

        n = get_object_provenance(obj)
        self._execute("UPDATE state SET record_type=? WHERE state_id=1", (n,))

    def _summary_not_completed(self) -> list[dict]:
        """returns a list of dicts summarising not completed results"""
        from scinexus.data_store import summary_not_completeds
        from scinexus.io import DEFAULT_DESERIALISER

        return summary_not_completeds(
            self.not_completed,
            deserialise=DEFAULT_DESERIALISER,
        )
