# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest


@pytest.mark.parametrize(
    ["max_trials", "success"],
    [(4, True), (5, False)],
)
def test_retry_decorator_max_trial(max_trials: int, success: bool):
    import time

    from beamlime.core.schedulers import retry

    interval = 0.2
    error_throw_ranges = iter(range(max_trials - int(success) * 1))
    context = {"run_counts": 0}

    @retry(TimeoutError, max_trials=max_trials, interval=interval)
    def something_takes_time():
        context["run_counts"] += 1
        for _ in error_throw_ranges:
            raise TimeoutError
        return True

    start_time = time.time()
    if not success:
        with pytest.raises(TimeoutError):
            something_takes_time()
    else:
        assert something_takes_time()

    time_consumed = time.time() - start_time
    expected_consumed_time = (max_trials - 1) * interval
    assert time_consumed > expected_consumed_time
    assert (
        time_consumed - expected_consumed_time
    ) < 0.05  # Expected delta for several trials.


@pytest.mark.parametrize(
    ["max_trials", "success"],
    [(4, True), (5, False)],
)
def test_async_retry_decorator_max_trial(max_trials: int, success: bool):
    import asyncio
    import time

    from beamlime.core.schedulers import async_retry

    interval = 0.2
    error_throw_ranges = iter(range(max_trials - int(success) * 1))
    context = {"run_counts": 0}

    @async_retry(TimeoutError, max_trials=max_trials, interval=interval)
    async def something_takes_time():
        context["run_counts"] += 1
        for _ in error_throw_ranges:
            raise TimeoutError
        return True

    start_time = time.time()
    if not success:
        with pytest.raises(TimeoutError):
            asyncio.run(something_takes_time())
    else:
        assert asyncio.run(something_takes_time())

    assert context["run_counts"] == max_trials
    time_consumed = time.time() - start_time
    expected_consumed_time = (max_trials - 1) * interval
    assert time_consumed > expected_consumed_time
    assert (
        time_consumed - expected_consumed_time
    ) < 0.05  # Expected delta for several trials.
