# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import inspect
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from functools import wraps

from ess.reduce.streaming import StreamProcessor

from beamlime.config.models import Parameter, WorkflowId, WorkflowSpec


class StreamProcessorFactory(Mapping[WorkflowId, WorkflowSpec]):
    def __init__(self) -> None:
        self._factories: dict[WorkflowId, Callable[[], StreamProcessor]] = {}
        self._workflow_specs: dict[WorkflowId, WorkflowSpec] = {}

    def __getitem__(self, key: WorkflowId) -> WorkflowSpec:
        return self._workflow_specs[key]

    def __iter__(self) -> Iterator[WorkflowId]:
        return iter(self._workflow_specs)

    def __len__(self) -> int:
        return len(self._workflow_specs)

    def register(
        self,
        name: str,
        description: str = '',
        source_names: Sequence[str] | None = None,
        parameters: Sequence[Parameter] | None = None,
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
        parameters:
            Optional list of parameters that the factory accepts. This is used to
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

            spec = WorkflowSpec(
                name=name,
                description=description,
                source_names=source_names or [],
                parameters=parameters or [],
            )
            spec_id = str(uuid.uuid4())
            self._factories[spec_id] = factory
            self._workflow_specs[spec_id] = spec
            return wrapper

        return decorator

    def create(
        self,
        *,
        workflow_id: WorkflowId,
        source_name: str,
        workflow_params: dict | None = None,
    ) -> StreamProcessor:
        """
        Create a StreamProcessor using the registered factory.

        Parameters
        ----------
        workflow_id:
            ID of the workflow to create.
        source_name:
            Name of the data source.
        workflow_params:
            Optional dictionary of parameter values to pass to the factory.
        """
        if workflow_id not in self._workflow_specs:
            raise KeyError(f"Unknown workflow ID: {workflow_id}")

        workflow_spec = self._workflow_specs[workflow_id]
        if workflow_spec.source_names and source_name not in workflow_spec.source_names:
            allowed_sources = ", ".join(workflow_spec.source_names)
            raise ValueError(
                f"Source '{source_name}' is not allowed for workflow "
                f"'{workflow_spec.name}'. "
                f"Allowed sources: {allowed_sources}"
            )

        factory = self._factories[workflow_id]
        sig = inspect.signature(factory)

        # Prepare arguments based on the factory signature
        kwargs = {}
        if 'source_name' in sig.parameters:
            kwargs['source_name'] = source_name

        # Add any additional workflow parameters if they match factory parameters
        if workflow_params:
            kwargs.update(workflow_params)

        # Call factory with appropriate arguments
        if kwargs:
            return factory(**kwargs)
        else:
            return factory()
