from unittest.mock import MagicMock, patch

import pytest

from scinexus.progress import (
    NoProgress,
    Progress,
    ProgressContext,
    RichProgress,
    TqdmProgress,
    get_progress,
    set_progress_backend,
)


@pytest.fixture(autouse=True)
def _reset_default():
    """Reset the module-level default after each test."""
    yield
    set_progress_backend(None)


def test_progress_abc_cannot_instantiate():
    with pytest.raises(TypeError):
        Progress()


def test_progress_abc_missing_call():
    class Incomplete(Progress):
        def child(self, *, leave=None):
            return self

    with pytest.raises(TypeError):
        Incomplete()


def test_progress_abc_missing_child():
    class Incomplete(Progress):
        def __call__(self, iterable, *, total=None, msg=""):
            yield from iterable

    with pytest.raises(TypeError):
        Incomplete()


def test_no_progress_yields_all_items_from_list():
    np = NoProgress()
    assert list(np([1, 2, 3])) == [1, 2, 3]


def test_no_progress_yields_all_items_from_generator():
    np = NoProgress()

    def gen():
        yield "a"
        yield "b"

    assert list(np(gen())) == ["a", "b"]


def test_no_progress_child_returns_self():
    np = NoProgress()
    assert np.child() is np


def test_no_progress_total_and_msg_accepted():
    np = NoProgress()
    assert list(np([1], total=1, msg="test")) == [1]


def test_no_progress_empty_iterable():
    np = NoProgress()
    assert list(np([])) == []


def test_no_progress_mixed_types():
    np = NoProgress()
    data = [1, "two", 3.0, None]
    assert list(np(data)) == data


def test_no_progress_is_progress_subclass():
    assert isinstance(NoProgress(), Progress)


def test_tqdm_yields_all_items():
    tp = TqdmProgress(disable=True)
    assert list(tp([1, 2, 3], total=3)) == [1, 2, 3]


def test_tqdm_yields_from_generator():
    tp = TqdmProgress(disable=True)

    def gen():
        yield "x"
        yield "y"

    assert list(tp(gen(), total=2)) == ["x", "y"]


def test_tqdm_default_position_is_zero():
    tp = TqdmProgress()
    assert tp._position == 0


def test_tqdm_child_increments_position():
    tp = TqdmProgress()
    child = tp.child()
    assert isinstance(child, TqdmProgress)
    assert child._position == 1


def test_tqdm_chained_child_positions():
    tp = TqdmProgress()
    grandchild = tp.child().child()
    assert grandchild._position == 2


def test_tqdm_empty_iterable():
    tp = TqdmProgress(disable=True)
    assert list(tp([], total=0)) == []


def test_tqdm_is_progress_subclass():
    assert isinstance(TqdmProgress(), Progress)


def test_tqdm_total_passed_to_tqdm():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress()
        list(tp([1, 2], total=42, msg="testing"))

        mock_tqdm.assert_called_once()
        call_kwargs = mock_tqdm.call_args
        assert call_kwargs.kwargs["total"] == 42
        assert call_kwargs.kwargs["desc"] == "testing"


def test_tqdm_leave_true_at_position_zero():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress()
        list(tp([], total=0))

        assert mock_tqdm.call_args.kwargs["leave"] is True


def test_tqdm_leave_false_at_position_nonzero():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress()
        child = tp.child()
        list(child([], total=0))

        assert mock_tqdm.call_args.kwargs["leave"] is False


def test_tqdm_custom_refresh_per_second():
    tp = TqdmProgress(refresh_per_second=5.0)
    assert tp._refresh_per_second == 5.0


@pytest.mark.parametrize("cls", [TqdmProgress, RichProgress])
@pytest.mark.parametrize("value", [0, -1.0])
def test_refresh_per_second_non_positive_raises(cls, value):
    with pytest.raises(ValueError, match="refresh_per_second must be positive"):
        cls(refresh_per_second=value)


def test_tqdm_custom_bar_format():
    tp = TqdmProgress(bar_format="{l_bar}{bar}")
    assert tp._bar_format == "{l_bar}{bar}"


def test_tqdm_extra_kwargs_stored():
    tp = TqdmProgress(unit="B")
    assert tp._tqdm_kwargs == {"unit": "B"}


def test_tqdm_child_inherits_options():
    tp = TqdmProgress(
        refresh_per_second=5.0,
        bar_format="{l_bar}",
    )
    child = tp.child()
    assert child._refresh_per_second == 5.0
    assert child._bar_format == "{l_bar}"


def test_tqdm_child_inherits_tqdm_kwargs():
    tp = TqdmProgress(unit="B")
    child = tp.child()
    assert child._tqdm_kwargs == {"unit": "B"}


def test_tqdm_options_passed_to_tqdm():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(
            refresh_per_second=5.0,
            bar_format="{l_bar}",
            bar_width=None,
        )
        list(tp([], total=0))

        kw = mock_tqdm.call_args.kwargs
        assert kw["mininterval"] == 0.2
        assert kw["bar_format"] == "{l_bar}"


def test_rich_yields_all_items():
    rp = RichProgress(disable=True)
    assert list(rp([1, 2, 3], total=3, msg="test")) == [1, 2, 3]


def test_rich_child_shares_progress_context():
    rp = RichProgress(disable=True)
    list(rp([1], total=1))
    child = rp.child()
    assert child._progress is rp._progress


def test_rich_custom_refresh_per_second():
    rp = RichProgress(refresh_per_second=5.0)
    assert rp._refresh_per_second == 5.0


def test_rich_child_inherits_refresh_per_second():
    rp = RichProgress(refresh_per_second=5.0)
    child = rp.child()
    assert child._refresh_per_second == 5.0


def test_rich_empty_iterable():
    rp = RichProgress(disable=True)
    assert list(rp([], total=0)) == []


def test_rich_is_progress_subclass():
    assert isinstance(RichProgress(), Progress)


def test_get_progress_false_returns_no_progress():
    assert isinstance(get_progress(show_progress=False), NoProgress)


def test_get_progress_true_returns_tqdm_progress():
    assert isinstance(get_progress(show_progress=True), TqdmProgress)


def test_get_progress_falsy_int_returns_no_progress():
    assert isinstance(get_progress(0), NoProgress)


def test_get_progress_passthrough_no_progress():
    np = NoProgress()
    assert get_progress(np) is np


def test_get_progress_passthrough_tqdm_progress():
    tp = TqdmProgress()
    assert get_progress(tp) is tp


def test_get_progress_default_arg_returns_no_progress():
    assert isinstance(get_progress(), NoProgress)


def test_set_default_no_progress_instance():
    np = NoProgress()
    set_progress_backend(np)
    assert isinstance(get_progress(show_progress=True), NoProgress)


def test_set_default_reset_with_none():
    set_progress_backend(NoProgress())
    set_progress_backend(None)
    assert isinstance(get_progress(show_progress=True), TqdmProgress)


def test_set_default_preserves_specific_instance():
    tp = TqdmProgress(refresh_per_second=5.0)
    set_progress_backend(tp)
    result = get_progress(show_progress=True)
    assert result is tp


def test_set_default_false_unaffected():
    set_progress_backend(TqdmProgress())
    assert isinstance(get_progress(show_progress=False), NoProgress)


def test_set_default_passthrough_unaffected():
    np = NoProgress()
    set_progress_backend(TqdmProgress())
    assert get_progress(np) is np


def test_set_default_string_tqdm():
    set_progress_backend("tqdm")
    assert isinstance(get_progress(show_progress=True), TqdmProgress)


def test_set_default_string_rich():
    set_progress_backend("rich")
    assert isinstance(get_progress(show_progress=True), RichProgress)


def test_set_default_invalid_string_raises():
    with pytest.raises(ValueError, match="unknown progress type"):
        set_progress_backend("invalid")


def test_set_default_string_tqdm_with_kwargs():
    set_progress_backend("tqdm", colour="green")
    result = get_progress(show_progress=True)
    assert isinstance(result, TqdmProgress)
    assert result._colour == "green"


def test_set_default_string_rich_with_kwargs():
    set_progress_backend("rich", colour="blue", leave=True)
    result = get_progress(show_progress=True)
    assert isinstance(result, RichProgress)
    assert result._colour == "blue"
    assert result._leave is True


def test_tqdm_nested_child_yields_all():
    outer = TqdmProgress(disable=True)
    inner = outer.child()
    outer_data = list(outer([1, 2], total=2))
    inner_data = list(inner([3, 4], total=2))
    assert outer_data == [1, 2]
    assert inner_data == [3, 4]


def test_no_progress_nested_chain():
    p = NoProgress()
    c = p.child()
    assert list(c([1, 2, 3])) == [1, 2, 3]
    assert c is p


def test_get_progress_child_type():
    tp = TqdmProgress()
    result = get_progress(tp).child()
    assert isinstance(result, TqdmProgress)


def test_partial_iteration_cleanup():
    tp = TqdmProgress(disable=True)
    it = tp([1, 2, 3, 4, 5], total=5)
    assert next(it) == 1
    assert next(it) == 2
    del it


def test_progress_context_abc_cannot_instantiate():
    with pytest.raises(TypeError):
        ProgressContext()


def test_progress_context_abc_missing_update():
    class Incomplete(ProgressContext):
        def close(self):
            pass

    with pytest.raises(TypeError):
        Incomplete()


def test_progress_context_close_default_is_noop():
    class MinimalCtx(ProgressContext):
        def update(self, *, progress, msg=""):
            pass

    ctx = MinimalCtx()
    ctx.close()


def test_no_progress_context_returns_progress_context():
    np = NoProgress()
    ctx = np.context()
    assert isinstance(ctx, ProgressContext)


def test_no_progress_context_update_is_noop():
    np = NoProgress()
    with np.context(msg="test") as ctx:
        ctx.update(progress=0.5, msg="halfway")


def test_no_progress_context_close_is_noop():
    np = NoProgress()
    ctx = np.context()
    ctx.close()
    ctx.close()


def test_tqdm_context_returns_progress_context():
    tp = TqdmProgress(disable=True)
    ctx = tp.context()
    assert isinstance(ctx, ProgressContext)
    ctx.close()


def test_tqdm_context_as_context_manager():
    tp = TqdmProgress(disable=True)
    with tp.context(msg="test") as ctx:
        ctx.update(progress=0.0, msg="start")
        ctx.update(progress=0.5, msg="halfway")
        ctx.update(progress=1.0, msg="done")


def test_tqdm_context_maps_start_end():
    tp = TqdmProgress(disable=True)
    with tp.context(start=0.0, end=0.9) as ctx:
        ctx.update(progress=0.5)
        assert ctx._bar.n == pytest.approx(0.45)


def test_tqdm_context_full_range():
    tp = TqdmProgress(disable=True)
    with tp.context(start=0.0, end=1.0) as ctx:
        ctx.update(progress=1.0)
        assert ctx._bar.n == pytest.approx(1.0)


def test_tqdm_context_subrange():
    tp = TqdmProgress(disable=True)
    with tp.context(start=0.9, end=1.0) as ctx:
        ctx.update(progress=0.5)
        assert ctx._bar.n == pytest.approx(0.95)


def test_tqdm_context_msg_updates_description():
    tp = TqdmProgress(disable=True)
    with tp.context() as ctx:
        ctx.update(progress=0.5, msg="custom message")
        assert "custom message" in ctx._bar.desc


def test_tqdm_context_no_msg_does_not_update_description():
    tp = TqdmProgress(disable=True)
    with tp.context(msg="initial") as ctx:
        ctx.update(progress=0.5, msg="updated")
        ctx.update(progress=0.7)


def test_tqdm_context_options_passed():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(refresh_per_second=5.0)
        ctx = tp.context(msg="test")

        kw = mock_tqdm.call_args.kwargs
        assert kw["total"] == 1.0
        assert kw["desc"] == "test"
        assert kw["mininterval"] == 0.2
        ctx.close()


def test_rich_context_returns_progress_context():
    rp = RichProgress(disable=True)
    ctx = rp.context()
    assert isinstance(ctx, ProgressContext)
    ctx.close()


def test_rich_context_as_context_manager():
    rp = RichProgress(disable=True)
    with rp.context(msg="test") as ctx:
        ctx.update(progress=0.5, msg="halfway")


def test_rich_context_maps_start_end():
    rp = RichProgress(disable=True)
    with rp.context(start=0.0, end=0.9) as ctx:
        ctx.update(progress=0.5)


def test_rich_context_creates_progress_on_first_call():
    rp = RichProgress(disable=True)
    assert rp._progress is None
    with rp.context() as ctx:
        ctx.update(progress=0.5)
    assert rp._progress is not None


def test_tqdm_context_multiphase():
    tp = TqdmProgress(disable=True)
    with tp.context() as ctx:
        for i in range(10):
            ctx.update(progress=i / 10 * 0.9, msg="Global")
        for i in range(10):
            ctx.update(progress=0.9 + i / 10 * 0.1, msg="Local")
        ctx.update(progress=1.0, msg="Done")
        assert ctx._bar.n == pytest.approx(1.0)


def test_tqdm_leave_none_uses_position_logic():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(leave=None)
        list(tp([], total=0))
        assert mock_tqdm.call_args.kwargs["leave"] is True

        child = tp.child(leave=None)
        list(child([], total=0))
        assert mock_tqdm.call_args.kwargs["leave"] is False


def test_tqdm_leave_true_overrides_position():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(leave=True)
        child = tp.child()
        list(child([], total=0))
        assert mock_tqdm.call_args.kwargs["leave"] is True


def test_tqdm_leave_false_overrides_position():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(leave=False)
        list(tp([], total=0))
        assert mock_tqdm.call_args.kwargs["leave"] is False


def test_tqdm_leave_propagated_to_child():
    tp = TqdmProgress(leave=True)
    child = tp.child()
    assert child._leave is True


def test_tqdm_child_leave_override():
    tp = TqdmProgress(leave=True)
    child = tp.child(leave=False)
    assert child._leave is False


def test_tqdm_child_leave_none_inherits():
    tp = TqdmProgress(leave=True)
    child = tp.child(leave=None)
    assert child._leave is True


def test_tqdm_context_respects_leave():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(leave=True)
        child = tp.child()
        ctx = child.context()
        assert mock_tqdm.call_args.kwargs["leave"] is True
        ctx.close()


def test_tqdm_colour_none_by_default():
    tp = TqdmProgress()
    assert tp._colour is None


def test_tqdm_colour_passed_to_tqdm():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(colour="green")
        list(tp([], total=0))
        assert mock_tqdm.call_args.kwargs["colour"] == "green"


def test_tqdm_colour_propagated_to_child():
    tp = TqdmProgress(colour="green")
    child = tp.child()
    assert child._colour == "green"


def test_tqdm_context_colour_passed():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(colour="blue")
        ctx = tp.context()
        assert mock_tqdm.call_args.kwargs["colour"] == "blue"
        ctx.close()


def test_rich_leave_false_by_default():
    rp = RichProgress()
    assert rp._leave is False


def test_rich_leave_false_removes_task_after_iteration():
    rp = RichProgress(disable=True, leave=False)
    result = list(rp([1, 2], total=2))
    assert result == [1, 2]
    assert len(rp._progress.tasks) == 0


def test_rich_leave_true_keeps_task():
    rp = RichProgress(disable=True, leave=True)
    result = list(rp([1, 2], total=2))
    assert result == [1, 2]
    assert len(rp._progress.tasks) == 1
    assert rp._progress.tasks[0].completed == 2


def test_rich_leave_propagated_to_child():
    rp = RichProgress(leave=True)
    child = rp.child()
    assert child._leave is True


def test_rich_child_leave_override():
    rp = RichProgress(leave=True)
    child = rp.child(leave=False)
    assert child._leave is False


def test_rich_child_leave_none_inherits():
    rp = RichProgress(leave=True)
    child = rp.child(leave=None)
    assert child._leave is True


def test_rich_context_leave_false_removes_task():
    rp = RichProgress(disable=True, leave=False)
    with rp.context(msg="test") as ctx:
        ctx.update(progress=0.5)
    assert len(rp._progress.tasks) == 0


def test_rich_context_leave_true_keeps_task():
    rp = RichProgress(disable=True, leave=True)
    with rp.context(msg="test") as ctx:
        ctx.update(progress=0.5)
    assert len(rp._progress.tasks) == 1
    assert rp._progress.tasks[0].completed == 1.0


def test_rich_colour_none_by_default():
    rp = RichProgress()
    assert rp._colour is None


def test_rich_colour_creates_styled_bar_column():
    from rich.progress import BarColumn  # type: ignore[import-not-found]

    rp = RichProgress(disable=True, colour="blue")
    rp._ensure_progress()
    bar_columns = [c for c in rp._progress.columns if isinstance(c, BarColumn)]
    assert len(bar_columns) == 1
    assert bar_columns[0].complete_style == "blue"
    assert bar_columns[0].finished_style == "blue"


def test_rich_colour_not_applied_when_progress_provided():
    from rich.progress import (  # type: ignore[import-not-found]
        BarColumn,
    )
    from rich.progress import (
        Progress as RProgress,
    )

    custom = RProgress(disable=True)
    rp = RichProgress(progress=custom, colour="red")
    result = rp._ensure_progress()
    assert result is custom
    bar_columns = [c for c in result.columns if isinstance(c, BarColumn)]
    for col in bar_columns:
        assert col.complete_style != "red"


def test_rich_colour_propagated_to_child():
    rp = RichProgress(colour="cyan")
    child = rp.child()
    assert child._colour == "cyan"


def test_rich_default_columns_include_elapsed_and_remaining():
    from rich.progress import (  # type: ignore[import-not-found]
        TimeElapsedColumn,
        TimeRemainingColumn,
    )

    rp = RichProgress(disable=True)
    rp._ensure_progress()
    column_types = [type(c) for c in rp._progress.columns]
    assert TimeElapsedColumn in column_types
    assert TimeRemainingColumn in column_types


def test_no_progress_child_accepts_leave():
    np = NoProgress()
    assert np.child(leave=True) is np
    assert np.child(leave=False) is np
    assert np.child(leave=None) is np


def test_tqdm_bar_width_default():
    tp = TqdmProgress()
    assert tp._bar_width is None


def test_tqdm_bar_width_passed_as_ncols():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(bar_width=80)
        list(tp([], total=0))
        kw = mock_tqdm.call_args.kwargs
        assert kw["ncols"] == 80
        assert kw["dynamic_ncols"] is False


def test_tqdm_bar_width_none_uses_dynamic_ncols():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(bar_width=None, dynamic_ncols=True)
        list(tp([], total=0))
        kw = mock_tqdm.call_args.kwargs
        assert "ncols" not in kw
        assert kw["dynamic_ncols"] is True


def test_tqdm_bar_width_propagated_to_child():
    tp = TqdmProgress(bar_width=80)
    child = tp.child()
    assert child._bar_width == 80


def test_rich_bar_width_default():
    rp = RichProgress()
    assert rp._bar_width is None


def test_rich_bar_width_applied_to_bar_column():
    from rich.progress import BarColumn  # type: ignore[import-not-found]

    rp = RichProgress(disable=True, bar_width=80)
    rp._ensure_progress()
    bar_columns = [c for c in rp._progress.columns if isinstance(c, BarColumn)]
    assert len(bar_columns) == 1
    assert bar_columns[0].bar_width == 80


def test_rich_bar_width_propagated_to_child():
    rp = RichProgress(bar_width=80)
    child = rp.child()
    assert child._bar_width == 80


def test_tqdm_reuses_bar_across_calls():
    tp = TqdmProgress(disable=True)
    assert list(tp([1, 2], total=2)) == [1, 2]
    assert list(tp([3, 4, 5], total=3)) == [3, 4, 5]


def test_tqdm_reuses_bar_single_creation():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress()
        list(tp([1], total=1))
        list(tp([2], total=1))

        mock_tqdm.assert_called_once()


def test_tqdm_reset_updates_total_and_msg():
    tp = TqdmProgress(disable=True)
    list(tp([], total=5, msg="first"))
    list(tp([], total=10, msg="second"))
    assert tp._bar.total == 10
    assert tp._bar.n == 0


def test_tqdm_del_closes_bar():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress()
        list(tp([], total=0))
        del tp

        mock_bar.close.assert_called_once()


def test_tqdm_del_without_use_is_safe():
    tp = TqdmProgress()
    del tp


def test_tqdm_print_after_single_bar_appears_after_bar():
    import io

    buf = io.StringIO()
    tp = TqdmProgress(leave=True, file=buf)
    list(tp([1, 2, 3], total=3, msg="step"))
    tp.close()
    print("DONE", file=buf)
    lines = buf.getvalue().splitlines()
    done_idx = next(i for i, ln in enumerate(lines) if "DONE" in ln)
    bar_idx = next(i for i, ln in enumerate(lines) if "step" in ln)
    assert done_idx > bar_idx


def test_tqdm_print_after_parent_and_child_bars_appears_after_bars():
    import io

    buf = io.StringIO()
    tp = TqdmProgress(leave=True, file=buf)
    child = tp.child(leave=True)
    for _ in tp([1, 2], total=2, msg="outer"):
        list(child([10, 20, 30], total=3, msg="inner"))
    tp.close()
    print("DONE", file=buf)
    lines = buf.getvalue().splitlines()
    done_idx = next(i for i, ln in enumerate(lines) if "DONE" in ln)
    outer_idx = next(i for i, ln in enumerate(lines) if "outer" in ln)
    inner_idx = next(i for i, ln in enumerate(lines) if "inner" in ln)
    assert done_idx > outer_idx
    assert done_idx > inner_idx


def test_tqdm_close_as_context_manager():
    import io

    buf = io.StringIO()
    with TqdmProgress(leave=True, file=buf) as tp:
        list(tp([1, 2, 3], total=3, msg="step"))
    print("DONE", file=buf)
    lines = buf.getvalue().splitlines()
    done_idx = next(i for i, ln in enumerate(lines) if "DONE" in ln)
    bar_idx = next(i for i, ln in enumerate(lines) if "step" in ln)
    assert done_idx > bar_idx


def test_tqdm_close_idempotent():
    import io

    buf = io.StringIO()
    tp = TqdmProgress(leave=True, file=buf)
    list(tp([1, 2], total=2, msg="step"))
    tp.close()
    tp.close()


def test_rich_close_stops_display():
    rp = RichProgress(disable=True, leave=True)
    list(rp([1, 2], total=2))
    assert rp._progress is not None
    rp.close()
    assert rp._task is None


def test_rich_close_as_context_manager():
    with RichProgress(disable=True, leave=True) as rp:
        list(rp([1, 2, 3], total=3))
    assert rp._task is None


def test_rich_close_idempotent():
    rp = RichProgress(disable=True, leave=True)
    list(rp([1, 2], total=2))
    rp.close()
    rp.close()


def test_rich_reuses_task_across_calls():
    rp = RichProgress(disable=True, leave=True)
    assert list(rp([1, 2], total=2)) == [1, 2]
    assert list(rp([3, 4, 5], total=3)) == [3, 4, 5]
    assert len(rp._progress.tasks) == 1


def test_rich_leave_false_removes_task_across_calls():
    rp = RichProgress(disable=True, leave=False)
    assert list(rp([1, 2], total=2)) == [1, 2]
    assert len(rp._progress.tasks) == 0
    assert list(rp([3, 4, 5], total=3)) == [3, 4, 5]
    assert len(rp._progress.tasks) == 0


def test_rich_reset_updates_total():
    rp = RichProgress(disable=True, leave=True)
    list(rp([1], total=1))
    list(rp([1, 2, 3], total=3))
    task = rp._progress.tasks[0]
    assert task.total == 3


def test_rich_del_completes_task_when_leave_true():
    rp = RichProgress(disable=True, leave=True)
    list(rp([1, 2], total=2))
    progress = rp._progress
    del rp
    assert len(progress.tasks) == 1
    assert progress.tasks[0].completed == 2


def test_rich_cleanup_removes_task_on_interrupted_iteration():
    rp = RichProgress(disable=True, leave=False)
    it = iter(rp([1, 2, 3], total=3))
    next(it)
    progress = rp._progress
    assert rp._task is not None
    rp._cleanup_task()
    assert rp._task is None
    assert len(progress.tasks) == 0


def test_rich_del_without_use_is_safe():
    rp = RichProgress()
    del rp


def test_child_reuses_bar_independently():
    outer = TqdmProgress(disable=True)
    child = outer.child()
    for batch in outer([1, 2], total=2):
        assert list(child([10, 20], total=2)) == [10, 20]


def test_no_progress_multiple_calls():
    np = NoProgress()
    assert list(np([1, 2])) == [1, 2]
    assert list(np([3, 4])) == [3, 4]


def test_get_progress_kwargs_forwarded():
    result = get_progress(show_progress=True, colour="green")
    assert isinstance(result, TqdmProgress)
    assert result._colour == "green"


def test_get_progress_kwargs_with_default_creates_new_instance():
    set_progress_backend("tqdm")
    result = get_progress(show_progress=True, colour="green")
    assert isinstance(result, TqdmProgress)
    assert result._colour == "green"


def test_get_progress_kwargs_with_rich_default():
    set_progress_backend("rich")
    result = get_progress(show_progress=True, colour="blue")
    assert isinstance(result, RichProgress)
    assert result._colour == "blue"


def test_get_progress_no_kwargs_returns_default():
    set_progress_backend("tqdm")
    default = get_progress(show_progress=True)
    assert default is get_progress(show_progress=True)


def test_get_progress_kwargs_with_instance_ignored():
    tp = TqdmProgress()
    assert get_progress(tp, colour="green") is tp


def test_set_progress_backend_rich_not_installed():
    """set_progress_backend("rich") raises ImportError when rich is missing"""
    with patch.dict("sys.modules", {"rich": None}), pytest.raises(ImportError):
        set_progress_backend("rich")


def test_get_progress_kwargs_false_ignored():
    result = get_progress(show_progress=False, colour="green")
    assert isinstance(result, NoProgress)


def test_get_progress_multiple_kwargs():
    result = get_progress(show_progress=True, colour="green", refresh_per_second=5.0)
    assert isinstance(result, TqdmProgress)
    assert result._colour == "green"
    assert result._refresh_per_second == 5.0
