# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from typing import NewType

import pytest
import sciline

from ess.livedata.handlers.stream_processor_workflow import StreamProcessorWorkflow

Streamed = NewType('Streamed', int)
Context = NewType('Context', int)
Static = NewType('Static', int)
ProcessedContext = NewType('ProcessedContext', int)
ProcessedStreamed = NewType('ProcessedStreamed', int)
Output = NewType('Output', int)


@pytest.fixture
def base_workflow_with_context() -> sciline.Pipeline:
    def make_static() -> Static:
        make_static.call_count += 1
        return Static(2)

    make_static.call_count = 0

    def process_context(context: Context, static: Static) -> ProcessedContext:
        process_context.call_count += 1
        return ProcessedContext(context * static)

    process_context.call_count = 0

    def process_streamed(
        streamed: Streamed, context: ProcessedContext
    ) -> ProcessedStreamed:
        return ProcessedStreamed(streamed + context)

    def finalize(streamed: ProcessedStreamed) -> Output:
        return Output(streamed)

    return sciline.Pipeline((make_static, process_context, process_streamed, finalize))


class TestStreamProcessorWorkflow:
    pass
