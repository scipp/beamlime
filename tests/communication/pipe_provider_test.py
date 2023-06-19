def test_pipe_instance_check():
    ...


def test_pipe_typehint_provider_registration():
    ...


def test_pipe_singleton_per_type():
    ...


def test_pipe_dependency_injection():
    from beamlime.communication import Pipe, PipeObject, pipe
    from beamlime.constructors import Container, Providers, local_providers

    with local_providers():
        p: Pipe[int] = Pipe(data_type=int)
        assert isinstance(p, PipeObject)
        assert isinstance(p, Pipe)
        assert isinstance(p, pipe)
        assert pipe(data_type=int) is pipe(data_type=int)
        assert Providers[Pipe[int]]() is pipe(data_type=int)
        assert Container[Pipe[int]] is p
