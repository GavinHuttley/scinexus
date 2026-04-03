from unittest.mock import MagicMock, patch

import pytest

from scinexus.progress import (
    NoProgress,
    Progress,
    ProgressContext,
    RichProgress,
    TqdmProgress,
    get_progress,
    set_default_progress,
)


@pytest.fixture(autouse=True)
def _reset_default():
    """Reset the module-level default after each test."""
    yield
    set_default_progress(None)


def test_progress_abc_cannot_instantiate():
    with pytest.raises(TypeError):
        Progress()


def test_progress_abc_missing_call():
    class Incomplete(Progress):
        def child(self):
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
    assert tp._position == 0  # noqa: SLF001


def test_tqdm_child_increments_position():
    tp = TqdmProgress()
    child = tp.child()
    assert isinstance(child, TqdmProgress)
    assert child._position == 1  # noqa: SLF001


def test_tqdm_chained_child_positions():
    tp = TqdmProgress()
    grandchild = tp.child().child()
    assert grandchild._position == 2  # noqa: SLF001


def test_tqdm_empty_iterable():
    tp = TqdmProgress(disable=True)
    assert list(tp([], total=0)) == []


def test_tqdm_is_progress_subclass():
    assert isinstance(TqdmProgress(), Progress)


def test_tqdm_total_passed_to_tqdm():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_bar.__iter__ = MagicMock(return_value=iter([1, 2]))
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
        mock_bar.__iter__ = MagicMock(return_value=iter([]))
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(position=0)
        list(tp([], total=0))

        assert mock_tqdm.call_args.kwargs["leave"] is True


def test_tqdm_leave_false_at_position_nonzero():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_bar.__iter__ = MagicMock(return_value=iter([]))
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(position=1)
        list(tp([], total=0))

        assert mock_tqdm.call_args.kwargs["leave"] is False


def test_tqdm_custom_mininterval():
    tp = TqdmProgress(mininterval=0.5)
    assert tp._mininterval == 0.5  # noqa: SLF001


def test_tqdm_custom_bar_format():
    tp = TqdmProgress(bar_format="{l_bar}{bar}")
    assert tp._bar_format == "{l_bar}{bar}"  # noqa: SLF001


def test_tqdm_extra_kwargs_stored():
    tp = TqdmProgress(ncols=80, colour="green")
    assert tp._tqdm_kwargs == {"ncols": 80, "colour": "green"}  # noqa: SLF001


def test_tqdm_child_inherits_options():
    tp = TqdmProgress(
        mininterval=0.5,
        bar_format="{l_bar}",
        dynamic_ncols=False,
    )
    child = tp.child()
    assert child._mininterval == 0.5  # noqa: SLF001
    assert child._bar_format == "{l_bar}"  # noqa: SLF001
    assert child._dynamic_ncols is False  # noqa: SLF001


def test_tqdm_child_inherits_tqdm_kwargs():
    tp = TqdmProgress(ncols=80)
    child = tp.child()
    assert child._tqdm_kwargs == {"ncols": 80}  # noqa: SLF001


def test_tqdm_options_passed_to_tqdm():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_bar.__iter__ = MagicMock(return_value=iter([]))
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(
            mininterval=2.0,
            bar_format="{l_bar}",
            dynamic_ncols=False,
            ncols=80,
        )
        list(tp([], total=0))

        kw = mock_tqdm.call_args.kwargs
        assert kw["mininterval"] == 2.0
        assert kw["bar_format"] == "{l_bar}"
        assert kw["dynamic_ncols"] is False
        assert kw["ncols"] == 80


def test_rich_yields_all_items():
    rp = RichProgress(disable=True)
    assert list(rp([1, 2, 3], total=3, msg="test")) == [1, 2, 3]


def test_rich_child_shares_progress_context():
    rp = RichProgress(disable=True)
    list(rp([1], total=1))
    child = rp.child()
    assert child._progress is rp._progress  # noqa: SLF001


def test_rich_custom_refresh_per_second():
    rp = RichProgress(refresh_per_second=5.0)
    assert rp._refresh_per_second == 5.0  # noqa: SLF001


def test_rich_child_inherits_refresh_per_second():
    rp = RichProgress(refresh_per_second=5.0)
    child = rp.child()
    assert child._refresh_per_second == 5.0  # noqa: SLF001


def test_rich_empty_iterable():
    rp = RichProgress(disable=True)
    assert list(rp([], total=0)) == []


def test_rich_is_progress_subclass():
    assert isinstance(RichProgress(), Progress)


def test_get_progress_false_returns_no_progress():
    assert isinstance(get_progress(False), NoProgress)


def test_get_progress_true_returns_tqdm_progress():
    assert isinstance(get_progress(True), TqdmProgress)


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
    set_default_progress(np)
    assert isinstance(get_progress(True), NoProgress)


def test_set_default_reset_with_none():
    set_default_progress(NoProgress())
    set_default_progress(None)
    assert isinstance(get_progress(True), TqdmProgress)


def test_set_default_preserves_specific_instance():
    tp = TqdmProgress(position=5)
    set_default_progress(tp)
    result = get_progress(True)
    assert result is tp


def test_set_default_false_unaffected():
    set_default_progress(TqdmProgress())
    assert isinstance(get_progress(False), NoProgress)


def test_set_default_passthrough_unaffected():
    np = NoProgress()
    set_default_progress(TqdmProgress())
    assert get_progress(np) is np


def test_set_default_string_tqdm():
    set_default_progress("tqdm")
    assert isinstance(get_progress(True), TqdmProgress)


def test_set_default_string_rich():
    set_default_progress("rich")
    assert isinstance(get_progress(True), RichProgress)


def test_set_default_invalid_string_raises():
    with pytest.raises(ValueError, match="unknown progress type"):
        set_default_progress("invalid")


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
        assert ctx._bar.n == pytest.approx(0.45)  # noqa: SLF001


def test_tqdm_context_full_range():
    tp = TqdmProgress(disable=True)
    with tp.context(start=0.0, end=1.0) as ctx:
        ctx.update(progress=1.0)
        assert ctx._bar.n == pytest.approx(1.0)  # noqa: SLF001


def test_tqdm_context_subrange():
    tp = TqdmProgress(disable=True)
    with tp.context(start=0.9, end=1.0) as ctx:
        ctx.update(progress=0.5)
        assert ctx._bar.n == pytest.approx(0.95)  # noqa: SLF001


def test_tqdm_context_msg_updates_description():
    tp = TqdmProgress(disable=True)
    with tp.context() as ctx:
        ctx.update(progress=0.5, msg="custom message")
        assert "custom message" in ctx._bar.desc  # noqa: SLF001


def test_tqdm_context_no_msg_does_not_update_description():
    tp = TqdmProgress(disable=True)
    with tp.context(msg="initial") as ctx:
        ctx.update(progress=0.5, msg="updated")
        ctx.update(progress=0.7)


def test_tqdm_context_options_passed():
    with patch("tqdm.auto.tqdm") as mock_tqdm:
        mock_bar = MagicMock()
        mock_tqdm.return_value = mock_bar

        tp = TqdmProgress(mininterval=2.0, dynamic_ncols=False)
        ctx = tp.context(msg="test")

        kw = mock_tqdm.call_args.kwargs
        assert kw["total"] == 1.0
        assert kw["desc"] == "test"
        assert kw["mininterval"] == 2.0
        assert kw["dynamic_ncols"] is False
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
    assert rp._progress is None  # noqa: SLF001
    with rp.context() as ctx:
        ctx.update(progress=0.5)
    assert rp._progress is not None  # noqa: SLF001


def test_tqdm_context_multiphase():
    tp = TqdmProgress(disable=True)
    with tp.context() as ctx:
        for i in range(10):
            ctx.update(progress=i / 10 * 0.9, msg="Global")
        for i in range(10):
            ctx.update(progress=0.9 + i / 10 * 0.1, msg="Local")
        ctx.update(progress=1.0, msg="Done")
        assert ctx._bar.n == pytest.approx(1.0)  # noqa: SLF001


def test_deprecation_progress_context():
    from scinexus.progress_display import ProgressContext

    with pytest.warns(DeprecationWarning, match="ProgressContext"):
        ProgressContext()


def test_deprecation_null_context():
    from scinexus.progress_display import NullContext

    with pytest.warns(DeprecationWarning, match="NullContext"):
        NullContext()


def test_deprecation_display_wrap():
    from scinexus.progress_display import display_wrap

    with pytest.warns(DeprecationWarning, match="display_wrap"):

        @display_wrap
        def my_func():
            return 42


def test_deprecation_null_context_attr():
    import scinexus.progress_display

    with pytest.warns(DeprecationWarning, match="NULL_CONTEXT"):
        _ = scinexus.progress_display.NULL_CONTEXT


def test_deprecation_log_file_output():
    from scinexus.progress_display import LogFileOutput

    with pytest.warns(DeprecationWarning, match="LogFileOutput"):
        LogFileOutput()
