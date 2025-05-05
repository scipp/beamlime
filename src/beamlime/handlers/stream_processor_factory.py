# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import inspect
import uuid
from collections.abc import Callable, Sequence
from functools import wraps

from ess.reduce.streaming import StreamProcessor

from beamlime.config.models import WorkflowId, WorkflowSpec


class StreamProcessorFactory:
    def __init__(self) -> None:
        self._factories: dict[WorkflowId, Callable[[], StreamProcessor]] = {}
        self._workflow_specs: dict[WorkflowId, WorkflowSpec] = {}

    def get_available(self) -> tuple[WorkflowId, ...]:
        """Return a tuple of available factory names."""
        return tuple(self._factories.keys())

    def register(
        self,
        name: str,
        description: str = '',
        source_names: Sequence[str] | None = None,
    ) -> Callable[[Callable[[], StreamProcessor]], Callable[[], StreamProcessor]]:
        """
        Decorator to register a factory function for creating StreamProcessors.

        Parameters
        ----------
        name:
            Name to register the factory under.
        description:
            Optional description of the factory.
        source_names:
            Optional list of source names that the factory can handle. This is used to
            create a workflow specification.

        Returns
        -------
        Decorator function that registers the factory and returns it unchanged.
        """

        def decorator(
            factory: Callable[[], StreamProcessor],
        ) -> Callable[[], StreamProcessor]:
            @wraps(factory)
            def wrapper() -> StreamProcessor:
                return factory()

            if name in self._factories:
                raise ValueError(f"Factory for {name} already registered")
            spec = WorkflowSpec(
                name=name,
                description=description,
                source_names=source_names or [],
                parameters=[],
            )
            spec_id = str(uuid.uuid4())
            self._factories[spec_id] = factory
            self._workflow_specs[spec_id] = spec
            return wrapper

        return decorator

    def create(self, *, workflow_id: WorkflowId, source_name: str) -> StreamProcessor:
        """Create a StreamProcessor using the registered factory."""
        factory = self._factories[workflow_id]
        sig = inspect.signature(factory)
        if 'source_name' in sig.parameters:
            return factory(source_name=source_name)
        else:
            return factory()
