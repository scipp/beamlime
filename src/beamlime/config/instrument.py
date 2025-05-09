# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ess.reduce.streaming import StreamProcessor

from beamlime.config.models import Parameter
from beamlime.handlers.stream_processor_factory import StreamProcessorFactory


class InstrumentRegistry(Mapping[str, 'Instrument']):
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

    def __init__(self) -> None:
        self._instruments: dict[str, Instrument] = {}

    def register(self, instrument: Instrument) -> None:
        """Register an instrument configuration."""
        if instrument.name in self._instruments:
            raise ValueError(f"Instrument {instrument.name} is already registered.")
        self._instruments[instrument.name] = instrument

    def __getitem__(self, name: str) -> Instrument:
        """Get an instrument by name (implements Mapping)."""
        return self._instruments[name]

    def __iter__(self) -> Iterator[str]:
        """Return an iterator over instrument names (implements Mapping)."""
        return iter(self._instruments)

    def __len__(self) -> int:
        """Return the number of registered instruments (implements Mapping)."""
        return len(self._instruments)

    def __contains__(self, name: str) -> bool:
        """Check if an instrument is registered."""
        return name in self._instruments


instrument_registry = InstrumentRegistry()


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
        return self.processor_factory.register(
            name=name,
            description=description,
            source_names=source_names,
            parameters=parameters,
        )
