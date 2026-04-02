"""Mypy plugin for scinexus.composable.define_app.

Reads the ``main()`` method signature on decorated classes and synthesises
the correct ``__call__`` return type so that ``reveal_type(app(x))`` works.
"""

from __future__ import annotations

from collections.abc import Callable

from mypy.nodes import ARG_POS, ARG_STAR, ARG_STAR2, Argument, Var
from mypy.plugin import ClassDefContext, Plugin
from mypy.plugins.common import add_method_to_class
from mypy.types import AnyType, TypeOfAny, UnionType
from mypy.types import Type as MypyType

DEFINE_APP_FULLNAME = "scinexus.composable.define_app"


def _get_main_return_type(ctx: ClassDefContext) -> MypyType | None:
    """Extract the return type from the ``main()`` method."""
    info = ctx.cls.info
    main_sym = info.names.get("main")
    if main_sym is None or main_sym.node is None:
        return None
    main_type = main_sym.node.type  # type: ignore[attr-defined]
    if main_type is None:
        return None
    return main_type.ret_type


def _define_app_hook(ctx: ClassDefContext) -> bool:
    """Add __call__ and __add__ to @define_app-decorated classes."""
    ret = _get_main_return_type(ctx)
    if ret is None:
        return True

    from mypy.types import Instance

    # Build NotCompleted union type
    not_completed_type: MypyType = AnyType(TypeOfAny.special_form)
    nc_info = ctx.api.lookup_fully_qualified_or_none("scinexus.composable.NotCompleted")
    if nc_info and nc_info.node:
        not_completed_type = Instance(nc_info.node, [])  # type: ignore[arg-type]

    return_type = UnionType([ret, not_completed_type])

    val_arg = Argument(
        Var("val", AnyType(TypeOfAny.explicit)),
        AnyType(TypeOfAny.explicit),
        None,
        ARG_POS,
    )
    args_arg = Argument(
        Var("args", AnyType(TypeOfAny.explicit)),
        AnyType(TypeOfAny.explicit),
        None,
        ARG_STAR,
    )
    kwargs_arg = Argument(
        Var("kwargs", AnyType(TypeOfAny.explicit)),
        AnyType(TypeOfAny.explicit),
        None,
        ARG_STAR2,
    )

    add_method_to_class(
        ctx.api,
        ctx.cls,
        "__call__",
        [val_arg, args_arg, kwargs_arg],
        return_type,
    )

    # Add ComposableApp as a base class so instances are recognised as
    # composable by mypy (the runtime decorator rebuilds the class with
    # ComposableApp as a base via types.new_class).
    composable_sym = ctx.api.lookup_fully_qualified_or_none(
        "scinexus.composable.ComposableApp"
    )
    if composable_sym and composable_sym.node:
        any_type = AnyType(TypeOfAny.explicit)
        base_type = Instance(composable_sym.node, [any_type, any_type])  # type: ignore[arg-type]
        if not any(
            isinstance(b, Instance)
            and b.type.fullname == "scinexus.composable.ComposableApp"
            for b in ctx.cls.info.bases
        ):
            ctx.cls.info.bases.append(base_type)
            ctx.cls.info.mro.insert(1, composable_sym.node)  # type: ignore[arg-type]

    return True


class SciNexusPlugin(Plugin):
    def get_class_decorator_hook_2(
        self, fullname: str
    ) -> Callable[[ClassDefContext], bool] | None:
        return _define_app_hook if fullname == DEFINE_APP_FULLNAME else None


def plugin(version: str) -> type[SciNexusPlugin]:  # noqa: ARG001
    return SciNexusPlugin
