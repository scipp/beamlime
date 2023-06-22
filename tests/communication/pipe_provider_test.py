def test_pipe_instance_check():
    ...


def test_pipe_typehint_provider_registration():
    ...


def test_pipe_singleton_per_type():
    ...


def test_pipe_dependency_injection():
    from beamlime.communication import Pipe
    from beamlime.communication.pipes import PipeLine
    from beamlime.ready_factory import pipe_factory

    p: Pipe[int] = PipeLine.build_pipe(Pipe[int])
    assert isinstance(p, Pipe)
    assert pipe_factory[Pipe[int]] is p
