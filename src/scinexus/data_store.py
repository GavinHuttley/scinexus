from __future__ import annotations

import contextlib
import inspect
import json
import re
import reprlib
from abc import ABC, abstractmethod
from collections import defaultdict
from enum import Enum
from fnmatch import fnmatchcase
from functools import singledispatch
from io import TextIOWrapper
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict, overload

from scitrack import get_text_hexdigest  # type: ignore[import-untyped]

from scinexus._sync import LockMixin
from scinexus.deserialise import deserialise_object
from scinexus.io_util import _compression_handlers, get_format_suffixes, open_
from scinexus.parallel import is_master_process

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable, Iterator
    from typing import Any, Self

    from citeable import CitationBase

NOT_COMPLETED_TABLE = "not_completed"
LOG_TABLE = "logs"
MD5_TABLE = "md5"

# used for log files, not-completed results
_special_suffixes = re.compile(r"\.(log|json)$")

CITATIONS_FILE = "bibliography.citations"

# a checksum file is named for the record it belongs to and for which of the
# two kinds that record is. LEGACY is what both kinds shared before, so a
# file still carrying it belongs to a record this store cannot identify
COMPLETED_CHECKSUM = "cmplt"
NOT_COMPLETED_CHECKSUM = "ncmplt"
LEGACY_CHECKSUM = "txt"


class ChecksumMigration(TypedDict):
    """what :meth:`DataStoreDirectory.migrate_checksums` did, and did not

    ``ambiguous`` is a stem both kinds of record carry, so the file cannot
    be attributed. ``orphaned`` is one no record carries. ``superseded`` is
    one whose record already has a checksum under the current name.
    """

    migrated: int
    ambiguous: list[str]
    orphaned: list[str]
    superseded: list[str]


def _record_stem(unique_id: str) -> str:
    """the identifier with its format and compression suffixes removed"""
    stem = Path(unique_id).name
    sfx, cmp = get_format_suffixes(stem)
    for part in (cmp, sfx):
        if part:
            stem = stem.removesuffix(f".{part}")
    return stem


def _is_record(name: str, suffix: str) -> bool:
    """whether a file of this name is a record stored under this suffix

    Notes
    -----
    The suffix may carry a trailing wildcard, so this is a pattern match
    rather than a comparison. fnmatchcase rather than Path.glob or
    fnmatch, both of which fold case on Windows: a scan expressed through
    either claimed an ID_0.FASTA there and not here, where _record_name
    puts the suffix on as written on every platform. A store whose
    membership depends on the platform cannot be reasoned about.
    """
    return fnmatchcase(name, f"*.{suffix}")


def _compressions(name: str) -> frozenset[str]:
    """the compression suffixes this name carries, wherever they sit

    Notes
    -----
    Split on the dot rather than read from Path.suffixes, which reports
    nothing for a leading-dot name: Path(".gz").suffixes is empty, so a
    ".gz" identifier read as carrying no compression at all and was
    stored as .gz.fasta, a plain file under a name claiming gzip.

    Everything before the first dot is the stem and is skipped, so an
    identifier of "gz" stays a record called gz and is not read as a
    claim about compression.
    """
    return frozenset(
        part.lower()
        for part in name.split(".")[1:]
        if part.lower() in _compression_handlers
    )


def _check_compression(unique_id: str, suffix: str) -> None:
    """raise if the identifier names a compression this suffix does not write

    Parameters
    ----------
    unique_id
        identifier as given by the caller
    suffix
        format suffix the record is about to be stored with

    Raises
    ------
    ValueError
        if the two name different compressions, or the identifier names
        one and the suffix does not

    Notes
    -----
    Only a write asks this. The store decides the name, so it cannot
    honour a compression the caller asks for and the suffix does not
    give: it would either write an uncompressed record under a name
    claiming otherwise, or a compressed one under a name its own scan
    could not match. Saying so beats picking one of those.

    Every component is examined, not just the last one. A name claims a
    compression wherever it carries one -- id_0.gz.fasta reads as gzip to
    anything working left to right, and the .gz would survive into the
    stem and so into the stored name.
    """
    asked = _compressions(Path(unique_id).name)
    stored = _compressions(f"x.{suffix}")
    # naming none is never a conflict: the suffix supplies whatever
    # compression there is, and the identifier is only asked not to
    # contradict it
    if not asked or asked == stored:
        return

    named = ", ".join(f".{c}" for c in sorted(asked))
    carries = ", ".join(f".{c}" for c in sorted(stored)) if stored else "no compression"
    msg = (
        f"identifier {unique_id!r} names {named}, but a record stored "
        f"as .{suffix} carries {carries}"
    )
    raise ValueError(msg)


def _checksum_name(unique_id: str, *, completed: bool) -> str:
    """the file a record's checksum is kept in"""
    suffix = COMPLETED_CHECKSUM if completed else NOT_COMPLETED_CHECKSUM
    return f"{_record_stem(unique_id)}.{suffix}"


def _legacy_checksum_name(unique_id: str) -> str:
    """the file a record's checksum was kept in before the kinds were named"""
    return f"{_record_stem(unique_id)}.{LEGACY_CHECKSUM}"


NoneType = type(None)


class Mode(Enum):
    r = "r"
    w = "w"
    a = "a"


APPEND = Mode.a
OVERWRITE = Mode.w
READONLY = Mode.r

# Summary display registry
_summary_display_func: Callable[..., Any] | None = None

# Unique-ID extractor registry
_id_from_source_func: Callable[..., Any] | None = None


def set_summary_display(func: Callable[..., Any] | None) -> None:
    """Set the function used to display data store summaries.

    Parameters
    ----------
    func
        A callable with signature ``func(data, *, name) -> Any`` where
        *data* is a ``dict`` or ``list[dict]`` and *name* identifies the
        summary method (e.g. ``"describe"``). Pass ``None`` to clear.
    """
    global _summary_display_func  # noqa: PLW0603
    _summary_display_func = func


def get_summary_display() -> Callable[..., Any] | None:
    """Return the currently registered summary display function, or ``None``."""
    return _summary_display_func


def _apply_summary_display(data: Any, *, name: str) -> Any:
    if _summary_display_func is not None:
        return _summary_display_func(data, name=name)
    return data


def _summary_property(data_method: Callable[..., Any]) -> property:
    """Create a property that delegates to a protected data method and applies display.

    The *data_method* should be a method defined on ``DataStoreABC`` (or a
    subclass) whose name starts with ``_``.  Subclasses customise the raw
    data by overriding the ``_``-prefixed method; the public property
    created here handles display wrapping automatically.
    """
    method_name = data_method.__name__
    public_name = method_name.removeprefix("_")

    def fget(self: DataStoreABC) -> Any:
        data = getattr(self, method_name)()
        return _apply_summary_display(data, name=public_name)

    return property(fget, doc=data_method.__doc__)


class DataMemberABC(ABC):
    """Abstract base class for DataMember

    A data member is a handle to a record in a DataStore. It has a reference
    to its data store and a unique identifier.
    """

    @property
    @abstractmethod
    def data_store(self) -> DataStoreABC: ...

    @property
    @abstractmethod
    def unique_id(self) -> str: ...

    def __str__(self) -> str:
        return self.unique_id

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(data_store={self.data_store.source}, unique_id={self.unique_id})"

    def read(self) -> str | bytes:
        return self.data_store.read(self.unique_id)

    def __eq__(self, other: object) -> bool:
        """to check equality of members and check existence of a
        member in a list of members"""
        return isinstance(other, type(self)) and (self.data_store, self.unique_id) == (
            other.data_store,
            other.unique_id,
        )

    @property
    def md5(self) -> str | None:
        return self.data_store.md5(self.unique_id)


class DataStoreABC(LockMixin, ABC):
    """Abstract base class for DataStore"""

    _init_vals: dict[str, Any]
    _completed: list[DataMemberABC]
    _not_completed: list[DataMemberABC]

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        obj = object.__new__(cls)

        init_sig = inspect.signature(cls.__init__)
        bargs = init_sig.bind_partial(cls, *args, **kwargs)
        bargs.apply_defaults()
        init_vals = bargs.arguments
        init_vals.pop("self", None)

        obj._init_vals = init_vals
        obj._completed = []
        obj._not_completed = []
        # here rather than __init__ so instances built by __new__ alone,
        # including those pickle reconstructs, still get one
        obj._cache_lock = cls.new_lock()
        return obj

    @property
    @abstractmethod
    def source(self) -> str | Path:
        """string that references connecting to data store, override in subclass constructor"""
        ...

    @property
    @abstractmethod
    def mode(self) -> Mode:
        """string that references datastore mode, override in subclass constructor"""
        ...

    @property
    @abstractmethod
    def limit(self) -> int | None: ...

    def __repr__(self) -> str:
        name = self.__class__.__name__
        construction = ", ".join(f"{k}={v}" for k, v in self._init_vals.items())
        return f"{name}({construction})"

    def __str__(self) -> str:
        members = self.members
        num = len(members)
        name = self.__class__.__name__
        sample = f"{members[:2]}..." if num > 2 else members
        return f"{num}x member {name}(source='{self.source}', members={sample})"

    @overload
    def __getitem__(self, index: int) -> DataMemberABC: ...
    @overload
    def __getitem__(self, index: slice) -> list[DataMemberABC]: ...
    def __getitem__(self, index: int | slice) -> DataMemberABC | list[DataMemberABC]:
        return self.members[index]

    def __len__(self) -> int:
        return len(self.members)

    def __contains__(self, identifier: object) -> bool:
        """whether relative identifier has been stored

        Notes
        -----
        A member id is composed with pathlib, so on Windows it reads
        not_completed\\nc1.json, and comparing it to the caller's string
        meant the forward slash form -- the one the docs use and the one
        anything written on POSIX produces -- matched nothing.

        The comparison is on Path.parts rather than on Path equality.
        Path equality is case insensitive on Windows, which would make
        ID_0.fasta and id_0.fasta the same record on one platform and
        not the other. Comparing the parts keeps the separator handling
        and leaves the case alone.
        """
        if not isinstance(identifier, str):
            return False

        wanted = Path(identifier).parts
        return any(Path(m.unique_id).parts == wanted for m in self)

    @abstractmethod
    def read(self, unique_id: str) -> str | bytes: ...

    def close(self) -> None:
        """release whatever the store holds open

        Notes
        -----
        Does nothing for a store that holds nothing, which is every one but
        :class:`DataStoreSqlite`. It is defined here so a caller can close
        what :func:`open_data_store` returned without knowing which backend
        it got.
        """

    @staticmethod
    def _append_once(
        current: list[DataMemberABC],
        member: DataMemberABC,
    ) -> None:
        """record a just-written member in the cache, at most once

        Parameters
        ----------
        current
            the cached list as it stands now
        member
            the member to record
        """
        # current already holds an equal member when a scan in the gap
        # between the file landing on disk and this call published one, and
        # when the record is being written a second time. only the first
        # rebinds the list, so identity cannot stand in for the O(n) test
        if member not in current:
            current.append(member)

    def _check_writable(self, unique_id: str) -> None:
        if self.mode is READONLY:
            msg = "datastore is readonly"
            raise OSError(msg)
        if unique_id in self and self.mode is APPEND:
            msg = "cannot overwrite existing record in append mode"
            raise OSError(msg)

    @abstractmethod
    def write(self, *, unique_id: str, data: str | bytes) -> None:
        self._check_writable(unique_id)

    @abstractmethod
    def write_not_completed(self, *, unique_id: str, data: str | bytes) -> None:
        self._check_writable(unique_id)

    @abstractmethod
    def write_log(self, *, unique_id: str, data: str | bytes) -> None:
        self._check_writable(unique_id)

    @property
    def members(self) -> list[DataMemberABC]:
        # one acquisition spanning both, so the halves describe the same
        # moment. write() takes a record out of not_completed and then puts
        # it into completed, so halves read either side of that pair of
        # steps account for it in neither
        with self._cache_lock:
            return self.completed + self.not_completed

    def __iter__(self) -> Iterator[DataMemberABC]:
        yield from self.members

    @property
    @abstractmethod
    def logs(self) -> list[DataMemberABC]: ...

    @property
    @abstractmethod
    def completed(self) -> list[DataMemberABC]: ...

    @property
    @abstractmethod
    def not_completed(self) -> list[DataMemberABC]: ...

    def _summary_logs(self) -> list[dict]:
        """returns a list of dicts summarising log files"""
        rows = []
        for record in self.logs:
            lines = str(record.read()).splitlines()
            first = lines.pop(0).split("\t")
            row = {"time": first[0], "name": record.unique_id}
            key: str | None = None
            mapped: dict[str, str] = {}
            for line in lines:
                parts = line.split("\t")[-1].split(" : ", maxsplit=1)
                if len(parts) == 1:
                    if key is None:
                        msg = "malformed log data: continuation line before any key"
                        raise ValueError(msg)
                    mapped[key] += parts[0]
                    continue

                key = parts[0]
                mapped[key] = parts[1]

            row["python_version"] = mapped["python"]
            row["who"] = mapped["user"]
            row["command"] = mapped["command_string"]
            row["composable"] = mapped.get("composable function", "")
            rows.append(row)
        return rows

    summary_logs = _summary_property(_summary_logs)

    def _summary_not_completed(self) -> list[dict]:
        """returns a list of dicts summarising not completed results"""
        return summary_not_completeds(self.not_completed)

    summary_not_completed = _summary_property(_summary_not_completed)

    def _describe(self) -> dict[str, object]:
        num_not_completed = len(self.not_completed)
        num_completed = len(self.completed)
        num_logs = len(self.logs)
        return {
            "completed": num_completed,
            "not_completed": num_not_completed,
            "logs": num_logs,
        }

    describe = _summary_property(_describe)

    @abstractmethod
    def drop_not_completed(self, *, unique_id: str | None = None) -> None: ...

    def _validate(self) -> dict[str, object]:
        members = self.members
        correct_md5 = len(members)
        missing_md5 = 0
        for m in members:
            data = m.read()
            md5 = self.md5(m.unique_id)
            if md5 is None:
                missing_md5 += 1
                correct_md5 -= 1
            elif md5 != get_text_hexdigest(data):
                correct_md5 -= 1

        incorrect_md5 = len(members) - correct_md5 - missing_md5

        return {
            "md5_correct": correct_md5,
            "md5_incorrect": incorrect_md5,
            "md5_missing": missing_md5,
            "md5_legacy": self._count_legacy_checksums(),
            "has_log": len(self.logs) > 0,
        }

    def _count_legacy_checksums(self) -> int:
        """how many checksum files are still under the name both kinds shared

        Notes
        -----
        Zero for a store that keeps no checksum files of its own.
        """
        return 0

    def validate(self) -> dict[str, object]:
        return _apply_summary_display(self._validate(), name="validate")

    @abstractmethod
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

    def write_citations(self, *, data: tuple[CitationBase, ...]) -> None:
        """Write citations to the data store. Subclasses should override."""
        if not data:
            return
        import warnings

        warnings.warn(
            f"{type(self).__name__!r} does not support saving citations",
            UserWarning,
            stacklevel=2,
        )

    def _summary_citations(self) -> list[dict]:
        """Return a list of dicts summarising stored citations."""
        if type(self)._load_citations is DataStoreABC._load_citations:
            import warnings

            warnings.warn(
                f"{type(self).__name__!r} does not support saving citations",
                UserWarning,
                stacklevel=2,
            )
        citations = self._load_citations()
        return [{"app": c.summary()[0], "citation": c.summary()[1]} for c in citations]

    summary_citations = _summary_property(_summary_citations)

    def write_bib(self, dest_path: str | Path) -> None:
        """Write stored citations as a BibTeX .bib file."""
        citations = self._load_citations()
        if not citations:
            import warnings

            warnings.warn(
                "No citations stored in this data store",
                UserWarning,
                stacklevel=2,
            )
            return
        from citeable import write_bibtex

        dest_path = Path(dest_path).expanduser().absolute()
        write_bibtex(citations, dest_path)

    def _load_citations(self) -> list[CitationBase]:
        """Load stored citations. Override in subclasses."""
        return []


class DataMember(DataMemberABC):
    """Generic DataMember class, bound to a data store. All read operations
    delivered by the parent."""

    def __init__(self, *, data_store: DataStoreABC, unique_id: str) -> None:
        self._data_store = data_store
        self._unique_id = str(unique_id)

    @property
    def data_store(self) -> DataStoreABC:
        return self._data_store

    @property
    def unique_id(self) -> str:
        return self._unique_id


def summary_not_completeds(
    not_completed: list[DataMemberABC],
    deserialise: Callable[..., Any] | None = None,
) -> list[dict]:
    """
    Parameters
    ----------
    not_completed
        list of DataMember instances for notcompleted records
    deserialise
        a callable for converting not completed contents, the result of member.read() must be a json string
    """
    err_pat = re.compile(r"[A-Z][a-z]+[A-Z][a-z]+\:.+")
    types = defaultdict(list)
    indices = "type", "origin"
    num_bytes = 0
    for member in not_completed:
        record = member.read()
        if deserialise:
            record = deserialise(record)
        if isinstance(record, bytes):
            num_bytes += 1
            continue
        record = deserialise_object(record)
        key = tuple(getattr(record, k, None) for k in indices)
        match = err_pat.findall(record.message)
        types[key].append([match[-1] if match else record.message, record.source])

    if num_bytes == len(not_completed):
        return []

    rows = []
    maxtring = reprlib.aRepr.maxstring
    reprlib.aRepr.maxstring = 45
    limit_len = 45
    for key in types:
        msg_list, src_list = list(zip(*types[key], strict=False))
        messages = reprlib.repr(", ".join(m.splitlines()[-1] for m in set(msg_list)))
        sources = ", ".join(s.splitlines()[-1] for s in src_list if s)
        if len(sources) > limit_len:
            idx = sources.rfind(",", None, limit_len) + 1
            idx = idx if idx > 0 else limit_len
            sources = f"{sources[:idx]} ..."
        row = {
            "type": getattr(key[0], "value", key[0]),
            "origin": key[1],
            "message": messages,
            "num": len(types[key]),
            "source": sources,
        }
        rows.append(row)
    reprlib.aRepr.maxstring = maxtring  # restoring original val
    return rows


def _tidy_and_check_suffix(suffix: str | None) -> str:
    """tidies suffix by removing leading wildcards and dots"""
    suffix = suffix or ""
    suffix = re.sub(r"^[\s.*]+", "", suffix)  # tidy the suffix
    if not suffix or suffix == "*":
        msg = "suffix is required for DataStoreDirectory and cannot be just a wildcard"
        raise ValueError(msg)

    return suffix


class DataStoreDirectory(DataStoreABC):
    """data store backed by a directory on the filesystem"""

    def __init__(
        self,
        source: str | Path,
        mode: Mode | str = READONLY,
        suffix: str | None = None,
        limit: int | None = None,
        verbose: bool = False,
    ) -> None:
        self._mode = Mode(mode)
        source = Path(source)
        self._source = source.expanduser()
        self.suffix = _tidy_and_check_suffix(suffix)
        self._verbose = verbose
        self._source_check_create(self._mode)
        self._limit = limit

    def __contains__(self, item: object) -> bool:
        if not isinstance(item, str):
            return False
        # an item naming a subdirectory is a member id, exact as given. the
        # completion below is for bare caller input such as "brca1"
        if not Path(item).parent.name and not _special_suffixes.search(item):
            item = f"{item}.{self.suffix}" if self.suffix not in item else item
        return super().__contains__(item)

    def _source_check_create(self, mode: Mode) -> None:
        if not is_master_process():
            return

        sub_dirs = [NOT_COMPLETED_TABLE, LOG_TABLE, MD5_TABLE]
        source = self.source
        if mode is READONLY:
            if not source.exists():
                msg = f"'{source}' does not exist"
                raise OSError(msg)
            return

        if not source.exists():
            source.mkdir(parents=True, exist_ok=True)

        for sub_dir in sub_dirs:
            (source / sub_dir).mkdir(parents=True, exist_ok=True)

    @property
    def source(self) -> Path:
        """path that references the data store"""
        return self._source

    @property
    def mode(self) -> Mode:
        """string that references datastore mode, override in subclass constructor"""
        return self._mode

    @property
    def limit(self) -> int | None:
        return self._limit

    def read(self, unique_id: str) -> str:
        """reads data corresponding to identifier"""
        with open_(self.source / unique_id) as infile:
            return infile.read()

    def drop_not_completed(self, *, unique_id: str | None = None) -> None:
        """remove not-completed records from the directory

        Parameters
        ----------
        unique_id
            if provided, only drop the record with this identifier,
            otherwise drop all not-completed records
        """
        # named by the rule that stored it, given the suffix
        # write_not_completed passes to _write
        target = self._record_name(unique_id, "json")[0] if unique_id else ""
        # members carry the subdirectory, so the comparison below needs the
        # same form. built once: write() drops a twin on every call
        wanted = str(Path(NOT_COMPLETED_TABLE) / target) if target else ""
        nc_dir = self.source / NOT_COMPLETED_TABLE
        md5_dir = self.source / MD5_TABLE
        # the removals and the reset are one region, so a scan cannot run
        # against a half-emptied directory. it does NOT stop a caller that
        # already holds the list from watching it shrink: the members are
        # removed from that list rather than it being rebound. nor is it
        # atomic -- an unlink can still raise and leave it torn
        with self._cache_lock:
            # built only if a file under the shared name turns up, which
            # takes a scan and is worth nothing in a store without one
            completed_stems: set[str] | None = None
            for m in list(self.not_completed):
                # exact: an endswith test also matches a record whose name
                # merely ends with this one, such as abc1.json for c1.json
                if wanted and m.unique_id != wanted:
                    continue

                file = nc_dir / Path(m.unique_id).name
                file.unlink()
                # a checksum is optional -- md5() returns None without one
                # and _validate counts it under md5_missing -- so a record
                # that has none is still droppable
                md5_file = md5_dir / _checksum_name(m.unique_id, completed=False)
                md5_file.unlink(missing_ok=True)
                # a file under the name both kinds shared is this record's
                # only when no completed record carries the stem. with one
                # there it cannot be attributed, and leaving it would have
                # it answer for that record once this one is gone
                legacy = md5_dir / _legacy_checksum_name(m.unique_id)
                if legacy.exists():
                    if completed_stems is None:
                        completed_stems = {
                            _record_stem(c.unique_id) for c in self.completed
                        }
                    if _record_stem(m.unique_id) not in completed_stems:
                        legacy.unlink()
                self.not_completed.remove(m)

            if not target:
                # reset _not_completed to force not_completed to rebuild it.
                # limit makes it a view, so the rebuild may show records
                # this pass was not asked about
                self._not_completed: list[DataMemberABC] = []

    @property
    def logs(self) -> list[DataMemberABC]:
        log_dir = self.source / LOG_TABLE
        return (
            [
                DataMember(data_store=self, unique_id=str(Path(LOG_TABLE) / m.name))
                for m in log_dir.glob("*")
            ]
            if log_dir.exists()
            else []
        )

    @property
    def completed(self) -> list[DataMemberABC]:
        # the lock spans check, scan and publish: a writer appending between
        # the scan and the publish would otherwise be discarded by it
        with self._cache_lock:
            if not self._completed:
                # built locally and assigned once: appending to
                # self._completed would publish a partly scanned list
                found: list[DataMemberABC] = []
                # counts what it keeps, not what it looked at: the scan
                # now sees every entry in the directory, and limit
                # truncates the members
                for m in self.source.glob("*"):
                    if not _is_record(m.name, self.suffix):
                        continue
                    found.append(DataMember(data_store=self, unique_id=m.name))
                    if self.limit and len(found) == self.limit:
                        break
                self._completed = found
            return self._completed

    @property
    def not_completed(self) -> list[DataMemberABC]:
        with self._cache_lock:
            if not self._not_completed:
                found: list[DataMemberABC] = []
                for m in (self.source / NOT_COMPLETED_TABLE).glob("*"):
                    if not _is_record(m.name, "json"):
                        continue
                    found.append(
                        DataMember(
                            data_store=self,
                            unique_id=str(Path(NOT_COMPLETED_TABLE) / m.name),
                        ),
                    )
                    if self.limit and len(found) == self.limit:
                        break
                self._not_completed = found
            return self._not_completed

    def _record_name(self, unique_id: str, suffix: str) -> tuple[str, str | None]:
        """the file name a record with this identifier is stored under

        Parameters
        ----------
        unique_id
            identifier as given by the caller
        suffix
            format suffix the record is stored with

        Returns
        -------
        the file name, and the compression suffix it carries if any

        Notes
        -----
        The suffix names the file, the identifier does not, so the
        completed records of a ``.fasta.gz`` store are all ``.fasta.gz``.
        Honouring the identifier instead let a store of ``.fasta`` hold an
        ``id_0.fasta.gz`` that its own scan, which asks for the store's
        suffix, could never match.

        The stem is the identifier with a trailing format or compression
        suffix taken off. A suffix is recognised as written, so in a
        ``.fasta`` store ``id_0.FASTA`` is not a respelling of
        ``id_0.fasta``: it keeps its extension and becomes
        ``id_0.FASTA.fasta``.
        """
        name = f"{_record_stem(unique_id)}.{suffix}"
        return name, get_format_suffixes(name)[1]

    def _write(
        self,
        *,
        subdir: str,
        unique_id: str,
        suffix: str,
        data: str,
    ) -> DataMember | None:
        given = unique_id
        unique_id, cmp = self._record_name(unique_id, suffix)
        member_id = str(Path(subdir) / unique_id)
        # super().write refuses a read only store and an APPEND overwrite,
        # and both are more fundamental than a complaint about the name,
        # so they answer first
        super().write(unique_id=member_id, data=data)
        _check_compression(given, suffix)
        # unique_id names a completed record whatever subdir holds, so this
        # can only speak for completed ones
        if not subdir and suffix != "log" and unique_id in self:
            return None
        newline = None if cmp else "\n"
        mode = "wt" if cmp else "w"
        with open_(self.source / subdir / unique_id, mode=mode, newline=newline) as out:
            out.write(data)

        if subdir == LOG_TABLE:
            return None
        if subdir == NOT_COMPLETED_TABLE:
            member = DataMember(
                data_store=self,
                unique_id=str(Path(NOT_COMPLETED_TABLE) / unique_id),
            )
        elif not subdir:
            member = DataMember(data_store=self, unique_id=unique_id)

        md5 = get_text_hexdigest(data)
        # named for the kind as well as the record, so a completed record
        # and a not-completed one of the same name no longer share a file.
        # they still share one with a compressed record of that name, since
        # the stem drops the compression suffix
        checksum = _checksum_name(unique_id, completed=not subdir)
        with open_(self.source / MD5_TABLE / checksum, mode="w") as out:
            out.write(md5)

        return member

    def write(self, *, unique_id: str, data: str) -> DataMember:  # type: ignore[override]
        """writes a completed record ending with .suffix

        Parameters
        ----------
        unique_id
            unique identifier
        data
            text data to be written

        Returns
        -------
        a member for this record

        Raises
        ------
        ValueError
            if unique_id names a compression this store does not write

        Notes
        -----
        Drops any not-completed member corresponding to this identifier

        The store's suffix names the file, so any format suffix on
        unique_id is replaced by it. A compression suffix is the one part
        that cannot be replaced silently, because it says how to read the
        record back, so an identifier naming one the store does not write
        is refused instead.
        """
        member = self._write(
            subdir="",
            unique_id=unique_id,
            suffix=self.suffix,
            data=data,
        )
        # one region: the record leaves not_completed and enters completed
        # together, so no reader finds it in neither
        with self._cache_lock:
            self.drop_not_completed(unique_id=unique_id)
            if member is not None:
                self._append_once(self._completed, member)
        return member  # type: ignore[return-value]

    def write_not_completed(self, *, unique_id: str, data: str) -> DataMember:  # type: ignore[override]
        """writes a not completed record as json

        Parameters
        ----------
        unique_id
            unique identifier
        data
            text data to be written

        Returns
        -------
        a member for this record

        Raises
        ------
        ValueError
            if unique_id names a compression. These records are written
            as plain json whatever the store's own suffix is
        """
        (self.source / NOT_COMPLETED_TABLE).mkdir(parents=True, exist_ok=True)
        member = self._write(
            subdir=NOT_COMPLETED_TABLE,
            unique_id=unique_id,
            suffix="json",
            data=data,
        )
        # never None for this subdir, but _write is typed to allow it
        if member is not None:
            with self._cache_lock:
                self._append_once(self._not_completed, member)
        return member  # type: ignore[return-value]

    def write_log(self, *, unique_id: str, data: str) -> None:  # type: ignore[override]
        """writes a log file

        Parameters
        ----------
        unique_id
            unique identifier
        data
            text data to be written

        Raises
        ------
        ValueError
            if unique_id names a compression. Logs are written as plain
            .log whatever the store's own suffix is
        """
        (self.source / LOG_TABLE).mkdir(parents=True, exist_ok=True)
        _ = self._write(subdir=LOG_TABLE, unique_id=unique_id, suffix="log", data=data)

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
        completed = Path(unique_id).parent.name != NOT_COMPLETED_TABLE
        path = self.source / MD5_TABLE / _checksum_name(unique_id, completed=completed)
        if path.exists():
            return path.read_text()

        # a store written before the kinds were named keeps both under one
        # name, so nothing has to be rewritten for it to be readable
        legacy = self.source / MD5_TABLE / _legacy_checksum_name(unique_id)
        return legacy.read_text() if legacy.exists() else None

    def _count_legacy_checksums(self) -> int:
        md5_dir = self.source / MD5_TABLE
        return sum(1 for p in md5_dir.glob("*") if _is_record(p.name, LEGACY_CHECKSUM))

    def migrate_checksums(self) -> ChecksumMigration:
        """rename checksum files that predate the two kinds being named

        Returns
        -------
        how many were renamed, and the stems of those that were not

        Notes
        -----
        A file under the shared name holds whichever of the two records
        wrote last, which was never recorded, so one can be attributed only
        when a single record carries its stem. The rest are reported and
        left alone rather than guessed at.

        Requires ``mode="w"``. Read-only cannot rewrite anything, and
        append undertakes not to touch what is already in the store, which
        is what renaming these does.
        """
        if self.mode is not OVERWRITE:
            msg = (
                "migrating checksums rewrites files already in the store, "
                'which needs mode="w"'
            )
            raise OSError(msg)

        md5_dir = self.source / MD5_TABLE
        # read from the directories rather than the member properties,
        # which limit truncates and which answer from a cache another
        # writer cannot have updated. attributing a file to a record that
        # is merely out of view is the guess this exists to avoid
        completed = {
            _record_stem(p.name)
            for p in self.source.glob("*")
            if _is_record(p.name, self.suffix)
        }
        not_completed = {
            _record_stem(p.name)
            for p in (self.source / NOT_COMPLETED_TABLE).glob("*")
            if _is_record(p.name, "json")
        }

        migrated = 0
        ambiguous: list[str] = []
        orphaned: list[str] = []
        superseded: list[str] = []
        legacies = (p for p in md5_dir.glob("*") if _is_record(p.name, LEGACY_CHECKSUM))
        for legacy in sorted(legacies):
            stem = legacy.name.removesuffix(f".{LEGACY_CHECKSUM}")
            kinds = (stem in completed, stem in not_completed)
            if all(kinds):
                ambiguous.append(stem)
                continue
            if not any(kinds):
                orphaned.append(stem)
                continue

            suffix = COMPLETED_CHECKSUM if kinds[0] else NOT_COMPLETED_CHECKSUM
            target = md5_dir / f"{stem}.{suffix}"
            # a file already under the current name was written for that
            # record by this version, so it is the authority. renaming over
            # it replaces a checksum known to be right with one that may
            # belong to a record since dropped
            if target.exists():
                superseded.append(stem)
                continue
            legacy.rename(target)
            migrated += 1

        return {
            "migrated": migrated,
            "ambiguous": ambiguous,
            "orphaned": orphaned,
            "superseded": superseded,
        }

    def write_citations(self, *, data: tuple[CitationBase, ...]) -> None:
        if not data:
            return
        from citeable import to_jsons

        path = self.source / CITATIONS_FILE
        path.write_text(to_jsons(data))

    def _load_citations(self) -> list[CitationBase]:
        from citeable import from_jsons

        path = self.source / CITATIONS_FILE
        if not path.exists():
            return []
        return from_jsons(path.read_text())


class ReadOnlyDataStoreZipped(DataStoreABC):
    """read-only data store backed by a zip archive"""

    def __init__(
        self,
        source: str | Path,
        mode: Mode | str = READONLY,
        suffix: str | None = None,
        limit: int | None = None,
        verbose: bool = False,
    ) -> None:
        self._mode = Mode(mode)
        if self._mode is not READONLY:
            msg = "this is a read only data store"
            raise ValueError(msg)

        self.suffix = _tidy_and_check_suffix(suffix)
        source = Path(source)
        self._source = source.expanduser()
        if not self._source.exists():
            msg = f"{self._source!s} does not exit"
            raise OSError(msg)

        self._verbose = verbose
        self._limit = limit

    @property
    def limit(self) -> int | None:
        return self._limit

    @property
    def mode(self) -> Mode:
        return self._mode

    @property
    def source(self) -> Path:
        return self._source

    def read(self, unique_id: str) -> str | bytes:
        """reads data corresponding to identifier from the zip archive"""
        import zipfile

        member_path = str(Path(self.source.stem, unique_id)).replace("\\", "/")
        with zipfile.ZipFile(self.source) as archive:
            raw = archive.open(member_path)
            wrapped = TextIOWrapper(raw, encoding="latin-1")
            return wrapped.read()

    def _iter_matches(self, subdir: str, suffix: str | None) -> Iterator[Path]:
        """archive entries under subdir, stored with this suffix

        Parameters
        ----------
        subdir
            directory the entry sits in. An empty string does not mean
            the archive root, it means no check at all, so entries at
            every depth are considered. That is pre-existing and is why
            a zipped store's completed can pick up a .json out of
            not_completed and report it twice
        suffix
            suffix the entry is stored under, None for any

        Notes
        -----
        A suffix rather than a glob pattern, because Path.match folds
        case on Windows and so gave an archive a different membership
        there than the same archive has here.
        """
        import zipfile

        with zipfile.ZipFile(self._source) as archive:
            names = archive.namelist()
            for name in names:
                p = Path(name)
                if subdir and p.parent.name != subdir:
                    continue
                if p.name.startswith("."):
                    continue
                if suffix is None or _is_record(p.name, suffix):
                    yield p

    @property
    def completed(self) -> list[DataMemberABC]:
        # this store is read only, so publishing once is already enough for
        # correctness. the lock collapses N concurrent readers re-parsing the
        # zip central directory into one scan, and keeps the four member
        # properties uniform
        with self._cache_lock:
            if not self._completed:
                found: list[DataMemberABC] = []
                num_matches = 0
                for name in self._iter_matches("", self.suffix):
                    num_matches += 1
                    member = DataMember(data_store=self, unique_id=name.name)
                    found.append(member)

                    if self.limit and num_matches >= self.limit:
                        break

                self._completed = found

            return self._completed

    @property
    def not_completed(self) -> list[DataMemberABC]:
        with self._cache_lock:
            if not self._not_completed:
                found: list[DataMemberABC] = []
                num_matches = 0
                nc_dir_path = Path(NOT_COMPLETED_TABLE)
                for name in self._iter_matches(NOT_COMPLETED_TABLE, "json"):
                    num_matches += 1
                    member = DataMember(
                        data_store=self,
                        unique_id=str(nc_dir_path / name.name),
                    )
                    found.append(member)
                    if self.limit and num_matches >= self.limit:
                        break

                self._not_completed = found

            return self._not_completed

    @property
    def logs(self) -> list[DataMemberABC]:
        log_dir = Path(LOG_TABLE)
        logs: list[DataMemberABC] = []
        for name in self._iter_matches(LOG_TABLE, None):
            m = DataMember(data_store=self, unique_id=str(log_dir / name.name))
            logs.append(m)
        return logs

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
        completed = Path(unique_id).parent.name != NOT_COMPLETED_TABLE
        md5_dir = Path(MD5_TABLE)
        # the legacy name second: an archive cannot be rewritten, so a
        # store zipped before the kinds were named falls back forever
        candidates = (
            _checksum_name(unique_id, completed=completed),
            _legacy_checksum_name(unique_id),
        )
        # one pass: _iter_matches opens the archive and reads its central
        # directory each time it is called, so asking it per candidate
        # would open the file twice to answer one question
        found = {
            name.name: name
            for name in self._iter_matches(MD5_TABLE, None)
            if name.name in candidates
        }
        for md5_name in candidates:
            if md5_name not in found:
                continue
            m = DataMember(data_store=self, unique_id=str(md5_dir / md5_name))
            result = m.read()
            return result if isinstance(result, str) else result.decode()
        return None

    def _count_legacy_checksums(self) -> int:
        return len(list(self._iter_matches(MD5_TABLE, LEGACY_CHECKSUM)))

    def drop_not_completed(self, *, unique_id: str | None = None) -> None:
        """not supported on read-only zip data stores"""
        msg = "zip data stores are read only"
        raise TypeError(msg)

    def write(self, *, unique_id: str, data: str | bytes) -> None:
        msg = "zip data stores are read only"
        raise TypeError(msg)

    def write_not_completed(self, *, unique_id: str, data: str | bytes) -> None:
        msg = "zip data stores are read only"
        raise TypeError(msg)

    def write_log(self, *, unique_id: str, data: str | bytes) -> None:
        msg = "zip data stores are read only"
        raise TypeError(msg)

    def write_citations(self, *, data: tuple[CitationBase, ...]) -> None:
        msg = "zip data stores are read only"
        raise TypeError(msg)

    def _load_citations(self) -> list[CitationBase]:
        import zipfile

        from citeable import from_jsons

        target = str(Path(self.source.stem, CITATIONS_FILE)).replace("\\", "/")
        try:
            with zipfile.ZipFile(self.source) as archive:
                data = archive.read(target).decode("utf-8")
            return from_jsons(data)
        except KeyError:
            return []


def get_unique_id(name: object) -> str | None:
    """strips any format suffixes from name"""
    if (name := get_data_source(name)) is None:
        return None
    suffixes = ".".join(sfx for sfx in get_format_suffixes(name) if sfx)
    return re.sub(rf"[.]{suffixes}$", "", name)


def set_id_from_source(func: Callable[..., Any] | None) -> None:
    """Register a custom function for extracting unique IDs from data objects.

    The registered function is consulted as the default by
    :meth:`AppBase.as_completed` and :meth:`WriterApp.apply_to` to derive a
    unique identifier for each input, and by :class:`NotCompleted` to
    normalise the ``source=`` keyword on error records. Pass ``None`` to
    clear the registration and restore the built-in :func:`get_unique_id`.

    Parameters
    ----------
    func
        A callable taking a single data object and returning a string
        identifier (or ``None`` if no identifier can be extracted). The
        callable must be picklable if scinexus apps will be executed in
        parallel via ``loky`` / MPI.

    Notes
    -----
    Per-call overrides via the ``id_from_source`` keyword on
    :meth:`as_completed` and :meth:`apply_to` still take precedence over
    the registered function. Register before constructing apps for the
    cleanest behaviour.
    """
    global _id_from_source_func  # noqa: PLW0603
    _id_from_source_func = func


def get_id_from_source() -> Callable[..., Any]:
    """Return the active unique-ID extractor.

    Returns the function previously passed to :func:`set_id_from_source`,
    or :func:`get_unique_id` if nothing has been registered.
    """
    return _id_from_source_func or get_unique_id


@singledispatch
def get_data_source(data: object) -> str | None:
    source = getattr(data, "source", None)
    return None if source is None else get_data_source(source)


@get_data_source.register
def _(data: str) -> str | None:
    return get_data_source(Path(data))


@get_data_source.register
def _(data: Path) -> str | None:
    return data.name


@get_data_source.register
def _(data: dict) -> str | None:
    try:
        source = data.get("info", {})["source"]
    except KeyError:
        source = data.get("source", None)  # noqa
    return get_data_source(source)


@get_data_source.register
def _(data: DataMemberABC) -> str | None:
    return str(data.unique_id)


def make_record_for_json(
    identifier: str, data: Any, completed: bool
) -> dict[str, object]:
    """returns a dict for storage as json"""
    with contextlib.suppress(AttributeError):
        data = data.to_rich_dict()

    data = json.dumps(data)
    return {"identifier": identifier, "data": data, "completed": completed}


def load_record_from_json(data: Any) -> tuple[str, Any, bool]:
    """returns identifier, data, completed status from json string"""
    if isinstance(data, str):
        data = json.loads(data)

    value = data["data"]
    if isinstance(value, str):
        with contextlib.suppress(json.JSONDecodeError):
            value = json.loads(value)
    return data["identifier"], value, data["completed"]
