from unittest.mock import MagicMock, patch

import pytest

from scinexus.progress import (
    NoProgress,
    Progress,
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


# ---- Progress ABC ----


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


# ---- NoProgress ----


class TestNoProgress:
    def test_yields_all_items_from_list(self):
        np = NoProgress()
        assert list(np([1, 2, 3])) == [1, 2, 3]

    def test_yields_all_items_from_generator(self):
        np = NoProgress()

        def gen():
            yield "a"
            yield "b"

        assert list(np(gen())) == ["a", "b"]

    def test_child_returns_self(self):
        np = NoProgress()
        assert np.child() is np

    def test_total_and_msg_accepted(self):
        np = NoProgress()
        assert list(np([1], total=1, msg="test")) == [1]

    def test_empty_iterable(self):
        np = NoProgress()
        assert list(np([])) == []

    def test_mixed_types(self):
        np = NoProgress()
        data = [1, "two", 3.0, None]
        assert list(np(data)) == data

    def test_is_progress_subclass(self):
        assert isinstance(NoProgress(), Progress)


# ---- TqdmProgress ----


class TestTqdmProgress:
    def test_yields_all_items(self):
        tp = TqdmProgress(disable=True)
        assert list(tp([1, 2, 3], total=3)) == [1, 2, 3]

    def test_yields_from_generator(self):
        tp = TqdmProgress(disable=True)

        def gen():
            yield "x"
            yield "y"

        assert list(tp(gen(), total=2)) == ["x", "y"]

    def test_default_position_is_zero(self):
        tp = TqdmProgress()
        assert tp._position == 0  # noqa: SLF001

    def test_child_increments_position(self):
        tp = TqdmProgress()
        child = tp.child()
        assert isinstance(child, TqdmProgress)
        assert child._position == 1  # noqa: SLF001

    def test_chained_child_positions(self):
        tp = TqdmProgress()
        grandchild = tp.child().child()
        assert grandchild._position == 2  # noqa: SLF001

    def test_empty_iterable(self):
        tp = TqdmProgress(disable=True)
        assert list(tp([], total=0)) == []

    def test_is_progress_subclass(self):
        assert isinstance(TqdmProgress(), Progress)

    def test_total_passed_to_tqdm(self):
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

    def test_leave_true_at_position_zero(self):
        with patch("tqdm.auto.tqdm") as mock_tqdm:
            mock_bar = MagicMock()
            mock_bar.__iter__ = MagicMock(return_value=iter([]))
            mock_tqdm.return_value = mock_bar

            tp = TqdmProgress(position=0)
            list(tp([], total=0))

            assert mock_tqdm.call_args.kwargs["leave"] is True

    def test_leave_false_at_position_nonzero(self):
        with patch("tqdm.auto.tqdm") as mock_tqdm:
            mock_bar = MagicMock()
            mock_bar.__iter__ = MagicMock(return_value=iter([]))
            mock_tqdm.return_value = mock_bar

            tp = TqdmProgress(position=1)
            list(tp([], total=0))

            assert mock_tqdm.call_args.kwargs["leave"] is False


# ---- TqdmProgress constructor options ----


class TestTqdmProgressOptions:
    def test_custom_mininterval(self):
        tp = TqdmProgress(mininterval=0.5)
        assert tp._mininterval == 0.5  # noqa: SLF001

    def test_custom_bar_format(self):
        tp = TqdmProgress(bar_format="{l_bar}{bar}")
        assert tp._bar_format == "{l_bar}{bar}"  # noqa: SLF001

    def test_extra_tqdm_kwargs_stored(self):
        tp = TqdmProgress(ncols=80, colour="green")
        assert tp._tqdm_kwargs == {"ncols": 80, "colour": "green"}  # noqa: SLF001

    def test_child_inherits_options(self):
        tp = TqdmProgress(
            mininterval=0.5,
            bar_format="{l_bar}",
            dynamic_ncols=False,
        )
        child = tp.child()
        assert child._mininterval == 0.5  # noqa: SLF001
        assert child._bar_format == "{l_bar}"  # noqa: SLF001
        assert child._dynamic_ncols is False  # noqa: SLF001

    def test_child_inherits_tqdm_kwargs(self):
        tp = TqdmProgress(ncols=80)
        child = tp.child()
        assert child._tqdm_kwargs == {"ncols": 80}  # noqa: SLF001

    def test_options_passed_to_tqdm(self):
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


# ---- RichProgress ----


class TestRichProgress:
    def test_yields_all_items(self):
        rp = RichProgress(disable=True)
        assert list(rp([1, 2, 3], total=3, msg="test")) == [1, 2, 3]

    def test_child_shares_progress_context(self):
        rp = RichProgress(disable=True)
        # trigger creation of the internal progress object
        list(rp([1], total=1))
        child = rp.child()
        assert child._progress is rp._progress  # noqa: SLF001

    def test_custom_refresh_per_second(self):
        rp = RichProgress(refresh_per_second=5.0)
        assert rp._refresh_per_second == 5.0  # noqa: SLF001

    def test_child_inherits_refresh_per_second(self):
        rp = RichProgress(refresh_per_second=5.0)
        child = rp.child()
        assert child._refresh_per_second == 5.0  # noqa: SLF001

    def test_empty_iterable(self):
        rp = RichProgress(disable=True)
        assert list(rp([], total=0)) == []

    def test_is_progress_subclass(self):
        assert isinstance(RichProgress(), Progress)


# ---- get_progress ----


class TestGetProgress:
    def test_false_returns_no_progress(self):
        assert isinstance(get_progress(False), NoProgress)

    def test_true_returns_tqdm_progress(self):
        assert isinstance(get_progress(True), TqdmProgress)

    def test_falsy_int_returns_no_progress(self):
        assert isinstance(get_progress(0), NoProgress)

    def test_passthrough_no_progress(self):
        np = NoProgress()
        assert get_progress(np) is np

    def test_passthrough_tqdm_progress(self):
        tp = TqdmProgress()
        assert get_progress(tp) is tp

    def test_default_arg_returns_no_progress(self):
        assert isinstance(get_progress(), NoProgress)


# ---- set_default_progress ----


class TestSetDefaultProgress:
    def test_set_no_progress_instance(self):
        np = NoProgress()
        set_default_progress(np)
        assert isinstance(get_progress(True), NoProgress)

    def test_reset_with_none(self):
        set_default_progress(NoProgress())
        set_default_progress(None)
        assert isinstance(get_progress(True), TqdmProgress)

    def test_preserves_specific_instance(self):
        tp = TqdmProgress(position=5)
        set_default_progress(tp)
        result = get_progress(True)
        assert result is tp

    def test_false_unaffected_by_default(self):
        set_default_progress(TqdmProgress())
        assert isinstance(get_progress(False), NoProgress)

    def test_passthrough_unaffected_by_default(self):
        np = NoProgress()
        set_default_progress(TqdmProgress())
        assert get_progress(np) is np

    def test_string_tqdm(self):
        set_default_progress("tqdm")
        assert isinstance(get_progress(True), TqdmProgress)

    def test_string_rich(self):
        set_default_progress("rich")
        assert isinstance(get_progress(True), RichProgress)

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError, match="unknown progress type"):
            set_default_progress("invalid")


# ---- Integration / nested usage ----


class TestIntegration:
    def test_tqdm_nested_child_yields_all(self):
        outer = TqdmProgress(disable=True)
        inner = outer.child()
        outer_data = list(outer([1, 2], total=2))
        inner_data = list(inner([3, 4], total=2))
        assert outer_data == [1, 2]
        assert inner_data == [3, 4]

    def test_no_progress_nested_chain(self):
        p = NoProgress()
        c = p.child()
        assert list(c([1, 2, 3])) == [1, 2, 3]
        assert c is p

    def test_get_progress_child_type(self):
        tp = TqdmProgress()
        result = get_progress(tp).child()
        assert isinstance(result, TqdmProgress)

    def test_partial_iteration_cleanup(self):
        tp = TqdmProgress(disable=True)
        it = tp([1, 2, 3, 4, 5], total=5)
        assert next(it) == 1
        assert next(it) == 2
        # abandon iteration — should not raise on cleanup
        del it


# ---- Deprecation stubs ----


class TestDeprecationStubs:
    def test_progress_context_deprecated(self):
        from scinexus.progress_display import ProgressContext

        with pytest.warns(DeprecationWarning, match="ProgressContext"):
            ProgressContext()

    def test_null_context_deprecated(self):
        from scinexus.progress_display import NullContext

        with pytest.warns(DeprecationWarning, match="NullContext"):
            NullContext()

    def test_display_wrap_deprecated(self):
        from scinexus.progress_display import display_wrap

        with pytest.warns(DeprecationWarning, match="display_wrap"):

            @display_wrap
            def my_func():
                return 42

    def test_null_context_attr_deprecated(self):
        import scinexus.progress_display

        with pytest.warns(DeprecationWarning, match="NULL_CONTEXT"):
            _ = scinexus.progress_display.NULL_CONTEXT

    def test_log_file_output_deprecated(self):
        from scinexus.progress_display import LogFileOutput

        with pytest.warns(DeprecationWarning, match="LogFileOutput"):
            LogFileOutput()
