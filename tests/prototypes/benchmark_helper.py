# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import NewType, Optional

TargetCounts = NewType("TargetCounts", int)
ResultCounts = NewType("ResultCounts", int)
Parameters = NewType("Parameters", dict)

TimeLapse = NewType("TimeLapse", list)


class BenchmarkConfig:
    target_count: Optional[int] = None
    result_count: Optional[int] = None
