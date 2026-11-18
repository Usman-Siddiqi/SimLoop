from __future__ import annotations

import pytest

from simloop._choices import ChoiceRandom, ChoiceSource, derive_seed


def test_same_seed_same_choices() -> None:
    a, b = ChoiceSource(7), ChoiceSource(7)
    assert [a.below(1000) for _ in range(50)] == [b.below(1000) for _ in range(50)]
    assert a.record == b.record


def test_different_seeds_differ() -> None:
    a, b = ChoiceSource(1), ChoiceSource(2)
    assert [a.below(1 << 30) for _ in range(10)] != [b.below(1 << 30) for _ in range(10)]


def test_below_stays_in_range_and_records() -> None:
    source = ChoiceSource(3)
    values = [source.below(5) for _ in range(500)]
    assert set(values) == {0, 1, 2, 3, 4}
    assert source.record == values


def test_bound_of_one_is_not_recorded() -> None:
    source = ChoiceSource(3)
    assert source.below(1) == 0
    assert source.record == []


def test_invalid_bound() -> None:
    with pytest.raises(ValueError, match="positive"):
        ChoiceSource(0).below(0)


def test_replay_reproduces_record() -> None:
    original = ChoiceSource(11)
    drawn = [original.below(n) for n in (3, 10, 100, 2, 7)]
    replayed = ChoiceSource(999, replay=original.record)
    assert [replayed.below(n) for n in (3, 10, 100, 2, 7)] == drawn


def test_replay_clamps_and_pads_with_zero() -> None:
    source = ChoiceSource(0, replay=[50, 1])
    assert source.below(10) == 9
    assert source.below(10) == 1
    assert source.below(10) == 0
    assert source.record == [9, 1, 0]


def test_zero_draws_are_the_simplest_choices() -> None:
    source = ChoiceSource(0, replay=[])
    assert source.unit() == 0.0
    assert source.uniform(2.0, 5.0) == 2.0
    assert source.chance(0.99) is False


def test_chance_extremes_draw_nothing() -> None:
    source = ChoiceSource(0)
    assert source.chance(0.0) is False
    assert source.chance(1.0) is True
    assert source.record == []


def test_chance_frequency() -> None:
    source = ChoiceSource(5)
    hits = sum(source.chance(0.25) for _ in range(4000))
    assert 850 < hits < 1150


def test_choice_random_routes_through_source() -> None:
    source = ChoiceSource(4)
    rng = ChoiceRandom(source)
    values = (rng.random(), rng.randint(1, 6), rng.choice("abc"), rng.getrandbits(8))
    assert len(source.record) >= 4
    replayed = ChoiceRandom(ChoiceSource(0, replay=source.record))
    assert (
        replayed.random(),
        replayed.randint(1, 6),
        replayed.choice("abc"),
        replayed.getrandbits(8),
    ) == values


def test_choice_random_ignores_reseeding_and_has_no_state() -> None:
    rng = ChoiceRandom(ChoiceSource(1))
    rng.seed(123)
    with pytest.raises(TypeError):
        rng.getstate()


def test_seed_derivation_and_draws_are_pinned() -> None:
    # Recorded seeds must keep meaning the same execution across releases and
    # Python versions. If this changes, every published reproduction breaks.
    assert derive_seed(0, "choices") == 0x62E9FBB20D11C701
    assert derive_seed(42, "choices") != derive_seed(42, "random")
    source = ChoiceSource(0)
    assert [source.below(1000) for _ in range(5)] == [869, 825, 460, 783, 487]
