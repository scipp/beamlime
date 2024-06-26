# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pathlib
import platform
import uuid
from dataclasses import dataclass
from typing import NamedTuple, NewType

import psutil

from beamlime.constructors import ProviderGroup

env_providers = ProviderGroup()

OperatingSystem = NewType("OperatingSystem", str)
OperatingSystemVersion = NewType("OperatingSystemVersion", str)
PlatformDesc = NewType("PlatformDesc", str)
MachineType = NewType("MachineType", str)  # Processor type.


class TotalMemory(NamedTuple):
    value: int
    unit: str


class PhysicalCpuCores(NamedTuple):
    """Physical number of CPU cores."""

    value: int
    unit: str


class LogicalCpuCores(NamedTuple):
    """Logical number of CPU cores."""

    value: int
    unit: str


class ProcessCpuAffinity(NamedTuple):
    """Number of process CPU affinities."""

    value: int
    unit: str


class CpuFrequency(NamedTuple):
    """CPU frequency profile."""

    current: int
    min: int
    max: int


class MaximumFrequency(NamedTuple):
    """Maximum frequency of CPU cores."""

    value: int
    unit: str


class MinimumFrequency(NamedTuple):
    """Minimum frequency of CPU cores."""

    value: int
    unit: str


env_providers[OperatingSystem] = platform.system
env_providers[OperatingSystemVersion] = platform.version
env_providers[PlatformDesc] = platform.platform
env_providers[MachineType] = platform.machine
env_providers[CpuFrequency] = psutil.cpu_freq


@env_providers.provider
def provide_totalmemory_gb() -> TotalMemory:
    return TotalMemory(int(psutil.virtual_memory().total / 10**9), 'GB')


@env_providers.provider
def provide_physical_cpu_cores() -> PhysicalCpuCores:
    """Physical number of CPU cores."""

    return PhysicalCpuCores(psutil.cpu_count(logical=False), 'counts')


@env_providers.provider
def provide_total_cpu_cores() -> LogicalCpuCores:
    """Logical number of CPU cores."""

    return LogicalCpuCores(psutil.cpu_count(logical=True), 'counts')


@env_providers.provider
def provide_process_cpu_affinity() -> ProcessCpuAffinity:
    """Process CPU affinity."""
    try:
        return ProcessCpuAffinity(len(psutil.Process().cpu_affinity() or []), 'counts')
    except AttributeError:
        # In MacOS, the `cpu_affinity` attribute is not available.
        # It is not easy to assign specific number of CPU cores to the process in MacOS.
        # Therefore we can assume that the process can use all available physical cores
        # when we analyze the benchmark results.
        return ProcessCpuAffinity(None, 'counts')


@env_providers.provider
def provide_maximum_cpu_frequency(cpu_freqency: CpuFrequency) -> MaximumFrequency:
    """Maximum frequency of CPU cores."""

    return MaximumFrequency(cpu_freqency.max, 'MHz')


@env_providers.provider
def provide_minimum_cpu_frequency(cpu_freqency: CpuFrequency) -> MinimumFrequency:
    """Minimum frequency of CPU cores."""

    return MinimumFrequency(cpu_freqency.min, 'MHz')


@env_providers.provider
@dataclass
class CPUSpec:
    """
    Collection of the CPU profile.
    Physical/logical CPU cores and min/max frequency.
    """

    physical_cpu_cores: PhysicalCpuCores
    logical_cpu_cores: LogicalCpuCores
    process_cpu_affinity: ProcessCpuAffinity
    maximum_frequency: MaximumFrequency
    minimum_frequency: MinimumFrequency


@env_providers.provider
@dataclass
class HardwareSpec:
    """
    Collection of the hardware profile.
    OS, OS version, platform, machine type, memory and processor(cpu) spec.
    """

    operating_system: OperatingSystem
    operating_system_version: OperatingSystemVersion
    platform_desc: PlatformDesc
    machine_type: MachineType
    total_memory: TotalMemory
    cpu_spec: CPUSpec


GitRootDir = NewType("GitRootDir", pathlib.Path)
BenchmarkRootDir = NewType("BenchmarkRootDir", pathlib.Path)
BenchmarkSessionID = NewType("BenchmarkSessionID", str)
GitCommitID = NewType("GitCommitID", str)
DateTimeSuffix = NewType("DateTimeSuffix", str)
BenchmarkTargetName = NewType("BenchmarkTargetName", str)
BenchmarkResultFilePath = NewType("BenchmarkResultFilePath", pathlib.Path)


env_providers[BenchmarkSessionID] = lambda: uuid.uuid4().hex


@env_providers.provider
def provide_git_root() -> GitRootDir:
    import subprocess

    command = ['git', 'rev-parse', '--show-toplevel']
    command_result = subprocess.run(command, stdout=subprocess.PIPE, text=True)  # noqa: S603
    git_root_path = pathlib.Path(command_result.stdout.removesuffix('\n'))
    return GitRootDir(git_root_path)


@env_providers.provider
def provide_git_commit_id() -> GitCommitID:
    import subprocess

    command = ['git', 'rev-parse', 'HEAD']
    command_result = subprocess.run(command, stdout=subprocess.PIPE, text=True)  # noqa: S603
    return GitCommitID(command_result.stdout.removesuffix('\n'))


@env_providers.provider
def provide_benchmark_root(git_root_path: GitRootDir) -> BenchmarkRootDir:
    """
    >>> provide_benchmark_root('./')
    PosixPath('.benchmarks')
    """
    return BenchmarkRootDir(git_root_path / pathlib.Path('.benchmarks'))


@env_providers.provider
def provide_now() -> DateTimeSuffix:
    from datetime import datetime, timezone

    return DateTimeSuffix(datetime.now(tz=timezone.utc).isoformat(timespec='seconds'))


@env_providers.provider
def provide_new_file_path(
    prefix_timestamp: DateTimeSuffix, bm_root_dir: BenchmarkRootDir
) -> BenchmarkResultFilePath:
    return BenchmarkResultFilePath(
        bm_root_dir / pathlib.Path(f'result-{prefix_timestamp}.json')
    )


@env_providers.provider
@dataclass
class BenchmarkEnvironment:
    benchmark_run_id: BenchmarkSessionID  # Unique ID for each benchmark test.
    git_commit_id: GitCommitID  # Git commit ID of the current implementation.
    timestamp: DateTimeSuffix
    hardware_spec: HardwareSpec
