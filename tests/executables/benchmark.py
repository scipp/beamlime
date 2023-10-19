# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# Executable module of ``tests.prototypes.benchmark_runner`` with GUI option.
import pathlib

from ..prototypes.benchmark_runner import create_benchmark_factory


def clear_all() -> None:
    import os

    import rich
    import rich.prompt
    import rich.text

    from ..prototypes.benchmark_env import BenchmarkRootDir

    factory = create_benchmark_factory()
    benchmark_root = factory[BenchmarkRootDir]
    file_names = [
        file_name
        for file_name in os.listdir(benchmark_root)
        if file_name.endswith('.json')
    ]

    if not file_names:
        rich.print(f"No benchmark results was found in {benchmark_root}.")
        return

    rich.print(rich.text.Text('Benchmark Results:', style='bold blue'))
    lines = rich.text.Lines(
        (rich.text.Text(f"  - {file_name}", style='bold') for file_name in file_names)
    )
    rich.print(lines)

    prompt = rich.prompt.Prompt()
    answer = prompt.ask(
        "\nBenchmark results above will be removed.",
        choices=['Y', 'y', 'N', 'n'],
        show_choices=True,
    )
    if answer in ('Y', 'y'):
        [os.remove(benchmark_root / file_name) for file_name in file_names]
        rich.print("Benchmark results removed.")


def show_target(target: pathlib.Path):
    import rich

    from ..prototypes.benchmark_runner import (
        BenchmarkFileManager,
        BenchmarkResultFilePath,
    )

    if not target.exists():
        raise FileNotFoundError(target, " not found.")
    factory = create_benchmark_factory()
    with factory.constant_provider(BenchmarkResultFilePath, target):
        file_manager = factory[BenchmarkFileManager]
        rich.print(file_manager.load())


def test_benchmark_runner():
    from ..prototypes.benchmark_runner import BenchmarkSession

    factory = create_benchmark_factory()
    runner = factory[BenchmarkSession]

    def sample_func(a):
        return a

    assert runner.run(sample_func, a=0) == 0
    assert runner.run(sample_func, a=1) == 1
    assert runner.run(lambda b: b, b=1) == 1
    runner.save()


def benchmark_runner_arg_parser():
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        '--clear', action='store_true', help="Clear all benchmark results."
    )
    parser.add_argument(
        '--show', dest='target', default=None, help="Print selected benchmark results."
    )
    parser.add_argument(
        '--test-run', action='store_true', help="Run a simple benhcmark test."
    )
    return parser


def benchmark_runner_textual_widget():
    from .benchmark_helper_gui import BenchmarkApp

    benchmark_app = BenchmarkApp()
    benchmark_app.run()


if __name__ == "__main__":
    parser = benchmark_runner_arg_parser()
    args = parser.parse_args()
    if args.clear:
        clear_all()
    elif args.target:
        show_target(pathlib.Path(args.target))
    elif args.test_run:
        test_benchmark_runner()
    else:
        benchmark_runner_textual_widget()
