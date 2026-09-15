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
    Returns
    -------
    Handle to a sqlite3 session
    """
    db = sqlite3.connect(
        f"file:{path}?mode=ro",
        isolation_level=None,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        uri=True,
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
    """data store backed by a SQLite database"""

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

    def _connection(self) -> sqlite3.Connection:
        """the connection, opened if it is not already, taking no lock

        Reading who holds the lock, and releasing one, both have to work on
        a store this session may not write to.
        """
        self._check_open()
        if self._db is None:
            db_func = open_sqlite_db_ro if self.mode is READONLY else open_sqlite_db_rw
            self._db = db_func(self.source)

        if self._db is None:
            msg = "database connection is unexpectedly None"
            raise ValueError(msg)
        return self._db

    @property
    def db(self) -> sqlite3.Connection:
        db = self._connection()
        # taking the lock is a separate step from opening, and is retried
        # until it succeeds. a refusal that left a connection behind would
        # be taken as proof of a lock by every access after it
        if not self._holds_lock:
            self.lock()
        return db

    def _init_log(self) -> None:
        timestamp = datetime.datetime.now(tz=datetime.UTC)
        self.db.execute(f"INSERT INTO {LOG_TABLE}(date) VALUES (?)", (timestamp,))
        self._log_id = self.db.execute(
            f"SELECT log_id FROM {LOG_TABLE} where date = ?",
            (timestamp,),
        ).fetchone()["log_id"]

    def close(self) -> None:
        """release the lock and the connection, ending the store's life"""
        db: sqlite3.Connection | None = getattr(self, "_db", None)
        if db is None:
            self._closed = True
            return
        try:
            # an explicit close is the only thing that releases the lock, so
            # one still set marks a store whose session ended another way
            self.unlock()
        finally:
            # in a finally so a store whose lock could not be released is
            # still shut, rather than left usable by the failure
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
            result = self.db.execute(cmnd, (uid_path.name,)).fetchone()
            return result["data"]

        cmnd = f"SELECT * FROM {LOG_TABLE} WHERE log_name = ?"
        result = self.db.execute(cmnd, (uid_path.name,)).fetchone()

        return result["data"]

    @property
    def completed(self) -> list[DataMemberABC]:
        if not self._completed:
            self._completed = self._select_members(
                table_name=RESULT_TABLE,
                is_completed=True,
            )
        return self._completed

    @property
    def not_completed(self) -> list[DataMemberABC]:
        """returns database records of type NotCompleted"""
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
        cmnd = self.db.execute(
            f"SELECT record_id FROM {table_name} WHERE is_completed=? {limit}",
            (is_completed,),
        )
        return [
            DataMember(data_store=self, unique_id=r["record_id"])
            for r in cmnd.fetchall()
        ]

    @property
    def logs(self) -> list[DataMemberABC]:
        """returns all log records"""
        cmnd = self.db.execute(f"SELECT log_name FROM {LOG_TABLE}")
        return [
            DataMember(data_store=self, unique_id=Path(LOG_TABLE) / r["log_name"])
            for r in cmnd.fetchall()
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
        """
        _check_identifier(unique_id)
        if self._log_id is None:
            self._init_log()

        if table_name == LOG_TABLE:
            # TODO how to evaluate whether writing a new log?
            cmnd = f"UPDATE {table_name} SET data =?, log_name =? WHERE log_id=?"
            self.db.execute(cmnd, (data, unique_id, self._log_id))
            return None

        md5 = get_text_hexdigest(data)

        if unique_id in self and self.mode is not APPEND:
            cmnd = f"UPDATE {table_name} SET data= ?, log_id=?, md5=? WHERE record_id=?"
            self.db.execute(cmnd, (data, self._log_id, md5, unique_id))
        else:
            cmnd = f"INSERT INTO {table_name} (record_id,data,log_id,md5,is_completed) VALUES (?,?,?,?,?)"
            self.db.execute(cmnd, (unique_id, data, self._log_id, md5, is_completed))

        return DataMember(data_store=self, unique_id=unique_id)

    def drop_not_completed(self, *, unique_id: str | None = None) -> None:
        """remove not-completed records from the database

        Parameters
        ----------
        unique_id
            if provided, only drop the record with this identifier,
            otherwise drop all not-completed records
        """
        vals: tuple[int] | tuple[int, str]
        if not unique_id:
            cmnd = f"DELETE FROM {RESULT_TABLE} WHERE is_completed=?"
            vals = (0,)
        else:
            cmnd = f"DELETE FROM {RESULT_TABLE} WHERE is_completed=? AND record_id=?"
            vals = (0, unique_id)
        self.db.execute(cmnd, vals)
        self._not_completed = []

    @property
    def _lock_id(self) -> int | str | None:
        """returns lock_pid: an owner token, or a bare pid from an older version

        Notes
        -----
        The first lock recorded, not the first row. A version that claimed
        the store as two statements rather than one transaction inserted a
        row per session, so a database written by it can carry a lock below
        a row holding none. ``IS NOT NULL`` rather than a truth test,
        because a lock recorded as 0 is a lock.
        """
        result = (
            self._connection()
            .execute(
                "SELECT lock_pid FROM state WHERE lock_pid IS NOT NULL "
                "ORDER BY state_id LIMIT 1",
            )
            .fetchone()
        )
        return result[0] if result else None

    @property
    def locked(self) -> bool:
        """returns if lock_pid is NULL or doesn't exist.

        Notes
        -----
        This reports whether the store is locked at all, not whether the
        caller is the one holding it. Compare ``_lock_id`` against
        ``_owner_token()`` for that.
        """
        return self._lock_id is not None

    def lock(self) -> None:
        """if writable, and not locked, locks the database to this session

        Notes
        -----
        Any lock already recorded is refused, whoever holds it, so a store
        is claimed by one session at a time whether the other is a thread
        of this process or another process entirely. Ownership is recorded
        as ``_owner_token()`` and decides only who may release it.
        """
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
            locked = self._claim(self._db)

        # a lock marks a store whose session did not end through close(), so
        # its records were never confirmed complete. no mode that writes may
        # build on that without being told to
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

    def _claim(self, db: sqlite3.Connection) -> int | str | None:
        """record this session as the owner, or report who already is

        Notes
        -----
        The read and the write are one transaction. As two statements on a
        connection in autocommit, sessions starting together each read an
        unlocked store and each wrote itself in. IMMEDIATE takes the write
        lock when the transaction opens rather than at its first write, so
        a second session waits there and then reads what the first
        committed. The statements are literal because ``isolation_level``
        is None, which leaves the DB-API transaction methods out of it.
        """
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
        """remove a lock this session took. If force, remove any. ignored if mode is READONLY

        Notes
        -----
        The owner recorded names a thread as well as a process, so a lock
        another thread of this process took is not this one's to release
        without *force*. Nor is one recorded by a version that wrote only a
        pid, which names a session this one cannot claim to be.

        Every lock recorded is cleared, not merely the one reported. A
        database written by the two-statement version can hold a lock in
        more than one row, and clearing them one call at a time leaves the
        store still refusing after the user has forced it open.
        """
        self._check_open()
        if self.mode is READONLY:
            return

        db = self._connection()
        lock_id = self._lock_id
        if lock_id is None:
            return

        # a lock recorded by an older version names only a process, which
        # this session cannot claim to be, so clearing one takes force
        if lock_id == _owner_token() or force:
            db.execute("UPDATE state SET lock_pid=NULL")
            self._holds_lock = False

        return

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
        result = self.db.execute(cmnd, (unique_id,)).fetchone()

        return result["md5"] if result else None

    def write_citations(self, *, data: tuple[CitationBase, ...]) -> None:
        if not data:
            return
        if not self._has_citations_table():
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS citations"
                "(citation_id INTEGER PRIMARY KEY, data TEXT)",
            )
        from citeable import to_jsons

        json_data = to_jsons(data)
        if existing := self.db.execute("SELECT citation_id FROM citations").fetchone():
            self.db.execute(
                "UPDATE citations SET data=? WHERE citation_id=?",
                (json_data, existing["citation_id"]),
            )
        else:
            self.db.execute("INSERT INTO citations(data) VALUES (?)", (json_data,))

    def _load_citations(self) -> list[CitationBase]:
        from citeable import from_jsons

        if not self._has_citations_table():
            return []
        result = self.db.execute("SELECT data FROM citations").fetchone()
        return from_jsons(result["data"]) if result else []

    def _has_citations_table(self) -> bool:
        result = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='citations'",
        ).fetchone()
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
        result = self.db.execute("SELECT record_type FROM state").fetchone()
        return result["record_type"]

    @record_type.setter
    def record_type(self, obj: object) -> None:
        from scinexus.misc import get_object_provenance

        rt = self.record_type
        if self.mode is OVERWRITE and rt:
            msg = f"cannot overwrite existing record_type {rt}"
            raise OSError(msg)

        n = get_object_provenance(obj)
        self.db.execute("UPDATE state SET record_type=? WHERE state_id=1", (n,))

    def _summary_not_completed(self) -> list[dict]:
        """returns a list of dicts summarising not completed results"""
        from scinexus.data_store import summary_not_completeds
        from scinexus.io import DEFAULT_DESERIALISER

        return summary_not_completeds(
            self.not_completed,
            deserialise=DEFAULT_DESERIALISER,
        )
