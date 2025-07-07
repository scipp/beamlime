# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import hashlib
import inspect
from collections.abc import Callable, Iterator, Mapping
from importlib import metadata

from ess.reduce.streaming import StreamProcessor

from beamlime.config.workflow_spec import WorkflowConfig, WorkflowId, WorkflowSpec


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

    @property
    def source_names(self) -> set[str]:
        """
        Get all source names that have associated workflows.

        Returns
        -------
        Set of source names.
        """
        return {
            source_name
            for spec in self._workflow_specs.values()
            for source_name in spec.source_names
        }

    def register(
        self, spec: WorkflowSpec
    ) -> Callable[[Callable[..., StreamProcessor]], Callable[..., StreamProcessor]]:
        """
        Decorator to register a factory function for creating StreamProcessors.

        Parameters
        ----------
        spec:
            Workflow specification that describes the workflow to register.

        Returns
        -------
        Decorator function that registers the factory and returns it unchanged.
        """
        spec_id = f"{spec.instrument}/{spec.name}/{spec.version}"

        def decorator(
            factory: Callable[..., StreamProcessor],
        ) -> Callable[..., StreamProcessor]:
            # Try to get the type hint of the 'params' argument if it exists
            sig = inspect.signature(factory)
            params_type = None
            params_param = sig.parameters.get('params')
            if params_param is not None:
                params_type = params_param.annotation
                if params_type is params_param.empty:
                    params_type = None
            spec.params = params_type
            self._factories[spec_id] = factory
            self._workflow_specs[spec_id] = spec
            return factory

        return decorator

    def create(self, *, source_name: str, config: WorkflowConfig) -> StreamProcessor:
        """
        Create a StreamProcessor using the registered factory.

        Parameters
        ----------
        source_name:
            Name of the data source.
        config:
            Configuration for the workflow, including the identifier and parameters.
        """
        workflow_id = config.identifier
        if workflow_id not in self._workflow_specs:
            raise KeyError(f"Unknown workflow ID: {workflow_id}")

        workflow_spec = self._workflow_specs[workflow_id]
        if (model_cls := workflow_spec.params) is None:
            workflow_params = None
        else:
            if config.params is None:
                raise ValueError(
                    f"Workflow '{workflow_spec.name}' requires parameters, "
                    f"but none were provided."
                )
            workflow_params = model_cls.model_validate(config.params)

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
        if workflow_params and 'params' in sig.parameters:
            kwargs['params'] = workflow_params

        # Call factory with appropriate arguments
        if kwargs:
            return factory(**kwargs)
        else:
            return factory()


def _hash_factory(factory: Callable[[], StreamProcessor]) -> str:
    """
    Create a simple hash of the factory function to use as a unique identifier.

    Note that this is currently not a full hash of the workflow or stream processor, but
    it should catch a fair number of changes, while enabling workflow recreation across
    service restarts. If the workflow or stream processor had incompatible changes, the
    worst that can happen is likely exceptions when running the workflow, e.g., from
    missing parameters.

    Parameters
    ----------
    factory:
        The factory function to hash.

    Returns
    -------
    :
        The hash of the factory function.
    """
    module_name = factory.__module__
    package_name = module_name.split('.')[0]
    qualname = factory.__qualname__
    try:
        version = metadata.version(package_name)
    except metadata.PackageNotFoundError:
        version = '0.0.0'
    source = inspect.getsource(factory)
    info = (
        module_name.encode()
        + package_name.encode()
        + qualname.encode()
        + version.encode()
        + source.encode()
    )
    return hashlib.sha256(info).hexdigest()
