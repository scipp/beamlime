# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections import UserDict
from collections.abc import Callable, Hashable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from beamlime.handlers.stream_processor_factory import StreamProcessorFactory

from .workflow_spec import WorkflowSpec


class InstrumentRegistry(UserDict[str, 'Instrument']):
    """
    Registry for instrument configurations.

    This class is used to register and retrieve instrument configurations
    based on their names. It allows for easy access to the configuration
    settings for different instruments.

    Note that in practice instruments are registered only when their module, creating
    an :py:class:`Instrument`, is imported. Beamlime does currently not import all
    instrument modules but only the requested one (since importing can be slow). This
    means that the registry will typically contain only a single instrument.
    """

    def register(self, instrument: Instrument) -> None:
        """Register an instrument configuration."""
        if instrument.name in self:
            raise ValueError(f"Instrument {instrument.name} is already registered.")
        self[instrument.name] = instrument


instrument_registry = InstrumentRegistry()


class StreamProcessor(Protocol):
    """
    Protocol matching ess.reduce.streaming.StreamProcessor, used by :py:class:`Job`.

    There will be other implementations, in particular for non-data-reduction jobs.
    """

    def accumulate(self, data: dict[Hashable, Any]) -> None: ...
    def finalize(self) -> dict[Hashable, Any]: ...
    def clear(self) -> None: ...


@dataclass(frozen=True, kw_only=True)
class Instrument:
    """
    Class for instrument configuration.

    This class is used to define the configuration for a specific instrument.
    It includes the stream mapping, processor factory, and other settings
    required for the instrument to function correctly.

    Instances are automatically registered with the global registry upon creation.
    """

    name: str
    processor_factory: StreamProcessorFactory = field(
        default_factory=StreamProcessorFactory
    )
    source_to_key: dict[str, type] = field(default_factory=dict)
    f144_attribute_registry: dict[str, dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Register the instrument with the global registry after initialization."""
        instrument_registry.register(self)

    def register_workflow(
        self,
        *,
        name: str,
        version: int,
        title: str,
        description: str = '',
        source_names: Sequence[str] | None = None,
    ) -> Callable[[Callable[..., StreamProcessor]], Callable[..., StreamProcessor]]:
        """
        Decorator to register a factory function for creating StreamProcessors.

        This decorator registers a factory function that creates a
        :py:class:`StreamProcessor` for a specific workflow. The decorator automatically
        registers the factory with the processor factory and returns the factory
        function unchanged.

        The factory function may have two parameters:
        - `source_name`: The name of the source to process.
        - `params`: A Pydantic model containing parameters for the workflow. The factory
          inspects the type hint of the `params` parameter to determine the correct
          model that the frontend uses to create workflow configuration widgets.

        Parameters
        ----------
        name:
            Name to register the workflow under.
        version:
            Version of the factory. This is used to differentiate between different
            versions of the same workflow.
        title:
            Title of the workflow. This is used for display in the UI.
        description:
            Optional description of the factory.
        source_names:
            Optional list of source names that the factory can handle. This is used to
            create a workflow specification.

        Returns
        -------
        Decorator function that registers the factory and returns it unchanged.
        """
        spec = WorkflowSpec(
            instrument=self.name,
            name=name,
            version=version,
            title=title,
            description=description,
            source_names=list(source_names or []),
            params=None,  # placeholder, filled in from type hint later
        )
        return self.processor_factory.register(spec)
