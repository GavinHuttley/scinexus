import inspect
import json
import re
import sys
import textwrap
import time
import traceback
import types
import typing
from collections.abc import Generator
from copy import deepcopy
from enum import Enum
from pathlib import Path
from typing import Generic, TypeVar
from uuid import uuid4

from citeable import Citation
from scitrack import CachingLogger  # type: ignore[import-untyped]
from typeguard import TypeCheckError, check_type

from scinexus import parallel as PAR
from scinexus import progress_display as UI
from scinexus import typing as snx_typing
from scinexus._version import __version__
from scinexus.deserialise import register_deserialiser
from scinexus.misc import docstring_to_summary_rest, get_object_provenance
from scinexus.typing import (
    check_type_compatibility,
    get_type_display_names,
    resolve_type_hint,
)

from .data_store import (
    DataMember,
    DataStoreABC,
    get_data_source,
    get_unique_id,
)

_builtin_seqs = list, set, tuple

T = TypeVar("T")
R = TypeVar("R")


def _make_logfile_name(process) -> str:
    text = re.split(r"\s+\+\s+", str(process))
    parts = []
    for part in text:
        if part.find("(") >= 0:
            part = part[: part.find("(")]
        parts.append(part)
    result = "-".join(parts)
    uid = str(uuid4())
    return f"{result}-{uid[:8]}.log"


def _get_origin(origin):
    return origin if type(origin) == str else origin.__class__.__name__


class NotCompleted(int):
    """results that failed to complete"""

    type: str
    origin: str
    message: str
    source: str | None

    def __new__(cls, type, origin, message, source=None):
        """
        Parameters
        ----------
        type : str
            examples are 'ERROR', 'FAIL'
        origin
            where the instance was created, can be an instance
        message : str
            descriptive message, succinct traceback
        source : str or instance with .source or .info.source attributes
            the data operated on that led to this result.
        """
        # TODO this approach to caching persistent arguments for reconstruction
        # is fragile. Need an inspect module based approach
        origin = _get_origin(origin)
        try:
            source = get_data_source(source)
        except Exception:
            source = None
        d = locals()
        d = {k: v for k, v in d.items() if k != "cls"}
        result = int.__new__(cls, False)
        args = tuple(d.pop(v) for v in ("type", "origin", "message"))
        result._persistent = args, d

        result.type = type
        result.origin = origin
        result.message = message
        result.source = source
        return result

    def __getnewargs_ex__(self, *args, **kw):
        return self._persistent[0], self._persistent[1]

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        name = self.__class__.__name__
        source = self.source or "Unknown"
        return f'{name}(type={self.type}, origin={self.origin}, source="{source}", message="{self.message}")'

    def to_rich_dict(self):
        """returns components for to_json"""
        return {
            "type": get_object_provenance(self),
            "not_completed_construction": {
                "args": self._persistent[0],
                "kwargs": self._persistent[1],
            },
            "version": __version__,
        }

    def to_json(self):
        """returns json string"""
        return json.dumps(self.to_rich_dict())


class AppType(Enum):
    LOADER = "loader"
    WRITER = "writer"
    GENERIC = "generic"
    NON_COMPOSABLE = "non_composable"


# Aliases to use Enum easily
LOADER = AppType.LOADER
WRITER = AppType.WRITER
GENERIC = AppType.GENERIC
NON_COMPOSABLE = AppType.NON_COMPOSABLE


def _get_raw_hints(main_func, min_params):
    _no_value = inspect.Parameter.empty
    params = inspect.signature(main_func)
    if len(params.parameters) < min_params:
        msg = f"{main_func.__name__!r} must have at least {min_params} input parameters"
        raise ValueError(
            msg,
        )
    # annotation for first parameter other than self, params.parameters is an orderedDict
    first_param_type = [p.annotation for p in params.parameters.values()][
        min_params - 1
    ]
    return_type = params.return_annotation
    if return_type is _no_value:
        msg = "must specify type hint for return type"
        raise TypeError(msg)
    if first_param_type is _no_value:
        msg = "must specify type hint for first parameter"
        raise TypeError(msg)

    if first_param_type is None:
        msg = "NoneType invalid type for first parameter"
        raise TypeError(msg)
    if return_type is None:
        msg = "NoneType invalid type for return value"
        raise TypeError(msg)

    if isinstance(first_param_type, str):
        msg = (
            "Apps do not yet support string type hints "
            "(such as those caused by __future__ annotations). "
            f"Bad type hint: {first_param_type}"
        )
        raise NotImplementedError(msg)
    if isinstance(return_type, str):
        msg = (
            "Apps do not yet support string type hints "
            "(such as those caused by __future__ annotations). "
            f"Bad type hint: {return_type}"
        )
        raise NotImplementedError(msg)

    return first_param_type, return_type


def _get_main_hints(klass: type) -> tuple:
    """return raw type hints for main method

    Returns
    -------
    (first_param_type_hint, return_type_hint)
    """
    # Check klass.main exists and is type method
    main_func = getattr(klass, "main", None)
    if (
        main_func is None
        or not inspect.isclass(klass)
        or not inspect.isfunction(main_func)
    ):
        msg = f"must define a callable main() method in {klass.__name__!r}"
        raise ValueError(msg)

    first_param_type, return_type = _get_raw_hints(main_func, 2)
    return first_param_type, return_type


def _set_hints(main_meth, first_param_type, return_type):
    """adds type hints to main"""
    main_meth.__annotations__["arg"] = first_param_type
    main_meth.__annotations__["return"] = return_type
    return main_meth


class source_proxy:
    __slots__ = ("_obj", "_src", "_uuid")

    def __init__(self, obj: typing.Any) -> None:
        self._obj = obj
        self._src = obj
        self._uuid = uuid4()

    def __hash__(self) -> int:
        return hash(self._uuid)

    @property
    def obj(self) -> typing.Any:
        return self._obj

    def set_obj(self, obj: typing.Any) -> None:
        self._obj = obj

    @property
    def source(self) -> typing.Any:
        """origin of this object"""
        return self._src

    @source.setter
    def source(self, src: typing.Any) -> None:
        # need to check whether src is hashable, how to cope if it isn't?
        # might need to make this instance hashable perhaps using a uuid?
        self._src = src

    @property
    def uuid(self) -> str:
        """unique identifier for this object"""
        return str(self._uuid)

    def __getattr__(self, name: str) -> typing.Any:
        return getattr(self._obj, name)

    def __setattr__(self, name: str, value: typing.Any) -> None:
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            setattr(self._obj, name, value)

    def __bool__(self) -> bool:
        return bool(self._obj)

    def __repr__(self) -> str:
        return self.obj.__repr__()

    def __str__(self) -> str:
        return self.obj.__str__()

    def __eq__(self, other: object) -> bool:
        return self.obj.__eq__(other)

    def __len__(self) -> int:
        return self.obj.__len__()

    # pickling induces infinite recursion on python 3.10
    # only on Windows, so implementing the following methods explicitly
    def __getstate__(self) -> tuple:
        return self._obj, self._src, self._uuid

    def __setstate__(self, state: tuple) -> None:
        self._obj, self._src, self._uuid = state


def _proxy_input(dstore) -> list:
    inputs = []
    for e in dstore:
        if not e:
            continue
        if not isinstance(e, source_proxy):
            e = e if hasattr(e, "source") else source_proxy(e)
        inputs.append(e)

    return inputs


GetIdFuncType = typing.Callable[[source_proxy | snx_typing.HasSource], str | None]


class propagate_source:
    """retains result association with source

    Notes
    -----
    Returns the unwrapped result if it has a .source instance,
    otherwise returns the original source_proxy with the .obj
    updated with result.
    """

    def __init__(self, app, id_from_source: GetIdFuncType) -> None:
        self.app = app
        self.id_from_source = id_from_source

    def __call__(
        self, value: source_proxy | snx_typing.HasSource
    ) -> snx_typing.HasSource:
        if not isinstance(value, source_proxy):
            return self.app(value)

        result = self.app(value.obj)
        if self.id_from_source(result):
            return result

        value.set_obj(result)
        return value


def _init_subclass_setup(cls, app_type, skip_not_completed, cite):
    """Shared setup logic for __init_subclass__ and define_app."""
    app_type = AppType(app_type)

    raw_input, raw_return = _get_main_hints(cls)
    mod = sys.modules.get(cls.__module__) if cls.__module__ else None
    module_globals = vars(mod) if mod else {}
    cls._input_type = resolve_type_hint(raw_input, module_globals)
    cls._return_type = resolve_type_hint(raw_return, module_globals)
    cls.app_type = app_type
    cls._skip_not_completed = skip_not_completed
    cls._cite = cite
    cls._source_wrapped = None

    if app_type is not LOADER:
        cls.input = None

    if hasattr(cls, "__slots__"):
        msg = "slots are not currently supported"
        raise NotImplementedError(msg)


class AppBase(Generic[T, R]):
    """Base for all app types. Provides __call__, __repr__, etc."""

    _is_intermediate_base: bool = False
    _skip_not_completed: bool
    _source_wrapped: propagate_source | None
    _cite: Citation | None
    _input_type: type
    _return_type: type
    _init_vals: dict
    app_type: AppType
    input: typing.Any
    main: typing.Callable

    def __init_subclass__(
        cls,
        app_type=GENERIC,
        skip_not_completed=True,
        cite=None,
        **kwargs,
    ):
        super().__init_subclass__(**kwargs)
        # Skip setup for intermediate bases and classes built by define_app
        if "_is_intermediate_base" in cls.__dict__ or getattr(
            cls, "_define_app_pending", False
        ):
            return
        _init_subclass_setup(cls, app_type, skip_not_completed, cite)

    def __new__(cls, *args, **kwargs):
        obj = object.__new__(cls)

        if hasattr(cls, "_func_sig"):
            # we have a decorated function, the first parameter in the signature
            # is not given to constructor, so we create a new signature excluding that one
            params = cls._func_sig.parameters
            init_sig = inspect.Signature(parameters=list(params.values())[1:])
            bargs = init_sig.bind_partial(*args, **kwargs)
        else:
            init_sig = inspect.signature(cls.__init__)
            bargs = init_sig.bind_partial(cls, *args, **kwargs)
        bargs.apply_defaults()
        init_vals = bargs.arguments
        init_vals.pop("self", None)

        obj._init_vals = init_vals
        return obj

    def __call__(self, val: T, *args, **kwargs) -> R | NotCompleted:
        if val is None:
            return NotCompleted(
                "ERROR", self, "unexpected input value None", source=val
            )

        if isinstance(val, NotCompleted) and self._skip_not_completed:
            return val

        if self.app_type is not LOADER and self.input:  # passing to connected app
            val = self.input(val, *args, **kwargs)
            if isinstance(val, NotCompleted) and self._skip_not_completed:
                return val

        type_checked = self._validate_data_type(val)
        if not type_checked:
            return type_checked

        try:
            result = self.main(val, *args, **kwargs)
        except Exception:
            result = NotCompleted("ERROR", self, traceback.format_exc(), source=val)

        if result is None:
            result = NotCompleted(
                "BUG", self, "unexpected output value None", source=val
            )
        return result

    def __repr__(self):
        val = f"{self.input!r} + " if self.app_type is not LOADER and self.input else ""
        all_args = {**self._init_vals}
        args_items = all_args.pop("args", None)
        data = ", ".join(f"{v!r}" for v in args_items) if args_items else ""
        kwargs_items = all_args.pop("kwargs", None)
        data += (
            ", ".join(f"{k}={v!r}" for k, v in kwargs_items.items())
            if kwargs_items
            else ""
        )
        data += ", ".join(f"{k}={v!r}" for k, v in all_args.items())
        data = f"{val}{self.__class__.__name__}({data})"
        return textwrap.fill(
            data, width=80, break_long_words=False, break_on_hyphens=False
        )

    __str__ = __repr__

    def _validate_data_type(self, data):
        """checks data type matches defined compatible types using typeguard"""
        if isinstance(data, NotCompleted):
            if self._skip_not_completed:
                return data
            # skip_not_completed=False means the app handles NotCompleted itself
            return True

        if isinstance(data, source_proxy):
            data = data.obj

        if isinstance(data, _builtin_seqs) and len(data) == 0:
            return NotCompleted("ERROR", self, message="empty data", source=data)

        try:
            check_type(data, self._input_type)
            return True
        except TypeCheckError:
            class_name = data.__class__.__name__
            expected = get_type_display_names(self._input_type)
            msg = f"invalid data type, '{class_name}' not in {', '.join(sorted(expected))}"
            return NotCompleted("ERROR", self, message=msg, source=data)

    @UI.display_wrap
    def as_completed(
        self,
        dstore,
        parallel: bool = False,
        par_kw: dict | None = None,
        id_from_source: GetIdFuncType = get_unique_id,
        **kwargs,
    ) -> Generator:
        """invokes self composable function on the provided data store

        Parameters
        ----------
        dstore
            a path, list of paths, or DataStore to which the process will be
            applied.
        parallel : bool
            run in parallel, according to arguments in par_kwargs. If True,
            the last step of the composable function serves as the master
            process, with earlier steps being executed in parallel for each
            member of dstore.
        par_kw
            dict of values for configuring parallel execution.
        kwargs
            setting a show_progress boolean keyword value here
            affects progress display code, other arguments are passed to
            the progress bar display_wrap decorator

        Notes
        -----
        If run in parallel, this instance serves as the master object and
        aggregates results. If run in serial, results are returned in the
        same order as provided.
        """
        if self._source_wrapped is None:
            app = propagate_source(
                self.input if self.app_type is WRITER else self, id_from_source
            )
        else:
            app = (
                self.input._source_wrapped
                if self.app_type is WRITER
                else self._source_wrapped
            )

        ui = kwargs.pop("ui")

        if isinstance(dstore, str):
            dstore = [dstore]
        elif isinstance(dstore, DataStoreABC):
            dstore = dstore.completed
        mapped = _proxy_input(dstore)
        if not mapped:
            return (_ for _ in ())

        if parallel:
            par_kw = par_kw or {}
            to_do: typing.Iterable = PAR.as_completed(app, mapped, **par_kw)
        else:
            to_do = map(app, mapped)

        return ui.series(to_do, count=len(mapped), **kwargs)

    def _get_citations(self) -> tuple[Citation, ...]:
        """Return citations for this app and all composed input apps."""
        seen: set[Citation] = set()
        result: list[Citation] = []

        if self._cite is not None:
            self._cite.app = self.__class__.__name__
            seen.add(self._cite)
            result.append(self._cite)

        head = getattr(self, "input", None)
        while head is not None:
            if head._cite is not None and head._cite not in seen:
                head._cite.app = head.__class__.__name__
                seen.add(head._cite)
                result.append(head._cite)
            head = getattr(head, "input", None)

        return tuple(result)

    @property
    def citations(self) -> tuple[Citation, ...]:
        """Citations for this app and all composed input apps."""
        return self._get_citations()

    @property
    def bib(self) -> str:
        """BibTeX formatted string of citations for this app and all composed input apps."""
        return "\n\n".join(str(cite) for cite in self.citations)


class ComposableApp(AppBase[T, R]):
    """Adds __add__ and disconnect for LOADER/GENERIC."""

    _is_intermediate_base: bool = True

    def __add__(self, other):
        if getattr(other, "app_type", None) not in {WRITER, LOADER, GENERIC}:
            msg = f"{other!r} is not composable"
            raise TypeError(msg)

        if other.input is not None:
            msg = f"{other.__class__.__name__} already part of composed function, use disconnect() to free them up"
            raise ValueError(
                msg,
            )

        if other is self:
            msg = "cannot add an app to itself"
            raise ValueError(msg)

        # Check order
        if self.app_type is WRITER:
            msg = "Left hand side of add operator must not be of type writer"
            raise TypeError(msg)
        if other.app_type is LOADER:
            msg = "Right hand side of add operator must not be of type loader"
            raise TypeError(msg)

        if not check_type_compatibility(self._return_type, other._input_type):
            self_names = get_type_display_names(self._return_type)
            other_names = get_type_display_names(other._input_type)
            msg = (
                f"{self.__class__.__name__!r} return_type {self_names} "
                f"incompatible with {other.__class__.__name__!r} input "
                f"type {other_names}"
            )
            raise TypeError(msg)
        other.input = self
        return other

    def disconnect(self) -> None:
        """resets input to None
        Breaks all connections among members of a composed function."""
        if self.app_type is LOADER:
            return
        if self.input:
            self.input.disconnect()

        self.input = None


class WriterApp(ComposableApp[T, R]):
    """Adds apply_to and set_logger for WRITER."""

    _is_intermediate_base: bool = True
    data_store: DataStoreABC
    logger: CachingLogger | None

    def apply_to(
        self,
        dstore,
        id_from_source: GetIdFuncType = get_unique_id,
        parallel: bool = False,
        par_kw: dict | None = None,
        logger: CachingLogger | None = None,
        cleanup: bool = True,
        show_progress: bool = False,
    ):
        """invokes self composable function on the provided data store

        Parameters
        ----------
        dstore
            a path, list of paths, or DataStore to which the process will be
            applied.
        id_from_source : callable
            makes the unique identifier from elements of dstore that will be
            used for writing results
        parallel : bool
            run in parallel, according to arguments in par_kwargs. If True,
            the last step of the composable function serves as the master
            process, with earlier steps being executed in parallel for each
            member of dstore.
        par_kw
            dict of values for configuring parallel execution.
        logger
            Argument ignored if not an io.writer. If a scitrack logger not provided,
            one is created with a name that defaults to the composable function names
            and the process ID.
        cleanup : bool
            after copying of log files into the data store, it is deleted
            from the original location
        show_progress : bool
            controls progress bar display

        Returns
        -------
        The output data store instance

        Notes
        -----
        This is an append only function, meaning that if a member already exists
        in self.data_store for an input, it is skipped.

        If run in parallel, this instance spawns workers and aggregates results.
        """
        if self.app_type is WRITER:
            if self.input is None:
                msg = "writer app has no composed input"
                raise RuntimeError(msg)
            self.input._source_wrapped = propagate_source(self.input, id_from_source)
            self._source_wrapped = propagate_source(self, id_from_source)

        if not self.input:
            msg = f"{self!r} is not part of a composed function"
            raise RuntimeError(msg)

        if isinstance(dstore, str | Path):  # one filename
            dstore = [dstore]
        elif isinstance(dstore, DataStoreABC):
            dstore = dstore.completed

        # TODO this should fail if somebody provides data that cannot produce a unique_id
        inputs = {}
        for m in dstore:
            input_id = Path(m.unique_id) if isinstance(m, DataMember) else m
            input_id = id_from_source(input_id)  # type: ignore[arg-type]
            if input_id in inputs or not input_id:
                msg = f"non-unique identifier {input_id!r} detected in data"
                raise ValueError(msg)
            if input_id in self.data_store:
                # we are assuming that this query returns True only when
                # an input_id is completed, we will not hit this if not_completed
                continue
            inputs[input_id] = m

        if (
            not dstore
        ):  # this should just return datastore, because if all jobs are done!
            msg = "dstore is empty"
            raise ValueError(msg)

        self.set_logger(logger)
        if self.logger:
            start = time.time()
            logger = self.logger
            logger.log_message(str(self), label="composable function")
            logger.log_versions(["scinexus"])

        proxied = _proxy_input(inputs.values())
        for result in self.as_completed(
            proxied,
            parallel=parallel,
            par_kw=par_kw,
            show_progress=show_progress,
        ):
            member = self.main(
                data=getattr(result, "obj", result),
                identifier=id_from_source(result),  # type: ignore[arg-type]
            )
            if self.logger:
                md5 = getattr(member, "md5", None)
                if logger is None:
                    msg = "logger is unexpectedly None"
                    raise RuntimeError(msg)
                logger.log_message(str(member), label="output")
                if md5:
                    logger.log_message(md5, label="output md5sum")

        if self.logger:
            if logger is None:
                msg = "logger is unexpectedly None"
                raise RuntimeError(msg)
            taken = time.time() - start
            logger.log_message(f"{taken}", label="TIME TAKEN")
            log_file_path = Path(logger.log_file_path)
            logger.shutdown()
            self.data_store.write_log(
                unique_id=log_file_path.name,
                data=log_file_path.read_text(),
            )
            if cleanup:
                log_file_path.unlink(missing_ok=True)

        # write citations
        self.data_store.write_citations(data=self.citations)

        return self.data_store

    def set_logger(self, logger=None) -> None:
        if logger is False:
            self.logger = None
            return
        if logger is None:
            logger = CachingLogger(create_dir=True)
        if not isinstance(logger, CachingLogger):
            msg = f"logger must be of type CachingLogger not {type(logger)}"
            raise TypeError(msg)
        if not logger.log_file_path:
            src = Path(self.data_store.source).parent  # type: ignore[attr-defined]
            logger.log_file_path = str(src / _make_logfile_name(self))
        self.logger = logger


# Keep module-level references for backwards compatibility (used in tests)
_add = ComposableApp.__add__


def _class_from_func(func):
    """make a class based on func

    Notes
    -----
    produces a class consistent with the necessary properties for
    the define_app class decorator.

    func becomes a static method on the class
    """

    # these methods MUST be in function scope so that separate instances are
    # created for each decorated function
    def _init(self, *args, **kwargs) -> None:
        self._args = args
        self._kwargs = kwargs
        self._source_wrapped = None

    def _main(self, arg, *args, **kwargs):
        kw_args = deepcopy(self._kwargs)
        kw_args = {**kw_args, **kwargs}
        args = (arg, *args, *deepcopy(self._args))
        bound = self._func_sig.bind(*args, **kw_args)
        return self._user_func(**bound.arguments)

    module = func.__module__  # to be assigned to the generated class
    sig = inspect.signature(func)
    class_name = func.__name__
    _main = _set_hints(_main, *_get_raw_hints(func, 1))
    summary, body = docstring_to_summary_rest(func.__doc__)
    func.__doc__ = None

    _class_dict = {"__init__": _init, "main": _main, "_user_func": staticmethod(func)}

    for method_name, method in _class_dict.items():
        method.__name__ = method_name
        method.__qualname__ = f"{class_name}.{method_name}"

    result = types.new_class(class_name, (), exec_body=lambda x: x.update(_class_dict))
    result.__module__ = module  # necessary for pickle support
    result._func_sig = sig
    result.__doc__ = summary
    result.__init__.__doc__ = body
    return result


# Forbidden methods per app kind
_FORBIDDEN_BASE = frozenset(
    {
        "__call__",
        "__repr__",
        "__str__",
        "__new__",
        "_validate_data_type",
    }
)
_FORBIDDEN_COMPOSABLE = _FORBIDDEN_BASE | frozenset(
    {
        "__add__",
        "disconnect",
        "input",
    }
)
_FORBIDDEN_WRITER = _FORBIDDEN_COMPOSABLE | frozenset(
    {
        "apply_to",
        "set_logger",
    }
)


def define_app(
    klass=None,
    *,
    app_type: AppType = GENERIC,
    skip_not_completed: bool = True,
    cite: Citation | None = None,
) -> type:
    """decorator for building callable apps

    Parameters
    ----------
    klass
        either a class or a function. If a function, it is converted to a
        class with the function bound as a static method.
    app_type
        what type of app, typically you just want GENERIC.
    skip_not_completed
        if True (default), NotCompleted instances are returned without being
        passed to the app.
    cite
        a Citation instance describing the software or algorithm. If provided,
        its ``.app`` attribute is set to the class name.

    Notes
    -----

    Instances of scinexus apps are callable. If an exception occurs,
    the app returns a ``NotCompleted`` instance with logging information.
    Apps defined with app_type ``LOADER``, ``GENERIC`` or ``WRITER`` can be
    "composed" (summed together) to produce a single callable that
    sequentially invokes the composed apps. For example, the independent
    usage of app instances ``app1`` and ``app2`` as

    .. code-block:: python

        app2(app1(data))

    is equivalent to

    .. code-block:: python

        combined = app1 + app2
        combined(data)

    The ``app_type`` attribute is used to constrain how apps can be composed.
    ``LOADER`` and ``WRITER`` are special cases. If included, a ``LOADER``
    must always be first, e.g.

    .. code-block:: python

        app = a_loader + a_generic

    If included, a ``WRITER`` must always be last, e.g.

    .. code-block:: python

        app = a_generic + a_writer

    Changing the order for either of the above will result in a ``TypeError``.

    There are no constraints on ordering of ``GENERIC`` aside from compatability of
    their input and return types (see below).

    In order to be decorated with ``@define_app`` a class **must**

    - implement a method called ``main``
    - type hint the first argument of ``main``
    - type hint the return type for ``main``

    While you can have more than one argument in ``main``, this is currently not
    supported in composable apps.

    Overlap between the return type hint and first argument hint is required
    for two apps to be composed together.

    ``define_app`` adds a ``__call__`` method which checks an input value prior
    to passing it to ``app.main()`` as a positional argument. The data checking
    results in ``NotCompleted`` being returned immediately, unless
    ``skip_not_completed==False``. If the input value type is consistent with
    the type hint on the first argument of main it is passed to ``app.main()``.
    If it does not match, a new ``NotCompleted`` instance is returned.

    Examples
    --------

    An example app definition.

    >>> from scinexus.composable import define_app

    >>> @define_app
    ... class noop:
    ...     def main(self, data: int) -> int:
    ...         return data
    """

    if hasattr(klass, "app_type"):
        msg = (
            f"The class {klass.__name__!r} is already decorated, avoid using "
            "inheritance from a decorated class."
        )
        raise TypeError(
            msg,
        )

    app_type = AppType(app_type)

    def wrapped(klass):
        if inspect.isfunction(klass):
            klass = _class_from_func(klass)
        if not inspect.isclass(klass):
            msg = f"{klass} is not a class"
            raise ValueError(msg)

        # Select base class based on app_type
        composable = app_type is not NON_COMPOSABLE
        if app_type is WRITER:
            base = WriterApp
            forbidden = _FORBIDDEN_WRITER
        elif composable:
            base = ComposableApp
            forbidden = _FORBIDDEN_COMPOSABLE
        else:
            base = AppBase
            forbidden = _FORBIDDEN_BASE

        # Check forbidden methods on the user's class
        if (
            composable
            and "input" in klass.__dict__
            and klass.__dict__["input"] is not None
        ):
            msg = f"remove 'input' attribute in {klass.__name__!r}, this functionality provided by define_app"
            raise TypeError(msg)
        for meth in forbidden:
            if meth in klass.__dict__ and inspect.isfunction(klass.__dict__[meth]):
                msg = f"remove {meth!r} in {klass.__name__!r}, this functionality provided by define_app"
                raise TypeError(msg)

        if hasattr(klass, "__slots__"):
            msg = "slots are not currently supported"
            raise NotImplementedError(msg)

        # Get type hints before rebuilding the class
        raw_input, raw_return = _get_main_hints(klass)

        # Collect the user's class dict (excluding metaclass artefacts)
        original_dict = {
            k: v
            for k, v in klass.__dict__.items()
            if k not in ("__dict__", "__weakref__")
        }
        # Prevent __init_subclass__ from running setup (we do it below)
        original_dict["_define_app_pending"] = True

        # Recreate class with the base (types.new_class stores
        # parameterised form in __orig_bases__ for type checkers)
        new_klass = types.new_class(
            klass.__name__,
            (base[raw_input, raw_return],),
            exec_body=lambda ns: ns.update(original_dict),
        )
        new_klass.__module__ = klass.__module__
        new_klass.__qualname__ = klass.__qualname__
        del new_klass._define_app_pending

        # Run setup once with the decorator's arguments
        _init_subclass_setup(new_klass, app_type, skip_not_completed, cite)

        return new_klass

    return wrapped(klass) if klass else wrapped  # type: ignore[return-value]


def is_app_composable(obj) -> bool:
    """checks whether obj has been decorated by define_app and it's app_type attribute is not NON_COMPOSABLE"""
    return is_app(obj) and obj.app_type is not NON_COMPOSABLE


def is_app(obj) -> bool:
    """checks whether obj has been decorated by define_app"""
    return hasattr(obj, "app_type")


@register_deserialiser(get_object_provenance(NotCompleted))
def deserialise_not_completed(data: dict) -> NotCompleted:
    """deserialising NotCompletedResult"""
    data.pop("version", None)
    init = data.pop("not_completed_construction")
    args = init.pop("args")
    kwargs = init.pop("kwargs")
    return NotCompleted(*args, **kwargs)
