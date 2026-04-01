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

DEFINE_APP_FULLNAME = "scinexus.composable.define_app"


def _get_main_return_type(ctx: ClassDefContext):
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
    """Add __call__ with correct return type to @define_app-decorated classes."""
    ret = _get_main_return_type(ctx)
    if ret is None:
        return True

    # Build NotCompleted union type
    not_completed_type = AnyType(TypeOfAny.special_form)
    nc_info = ctx.api.lookup_fully_qualified_or_none("scinexus.composable.NotCompleted")
    if nc_info and nc_info.node:
        from mypy.types import Instance

        not_completed_type = Instance(nc_info.node, [])  # type: ignore[assignment,arg-type]

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
    return True


class SciNexusPlugin(Plugin):
    def get_class_decorator_hook_2(
        self, fullname: str
    ) -> Callable[[ClassDefContext], bool] | None:
        return _define_app_hook if fullname == DEFINE_APP_FULLNAME else None


def plugin(version: str) -> type[SciNexusPlugin]:  # noqa: ARG001
    return SciNexusPlugin
