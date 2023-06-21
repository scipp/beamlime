def test_pipe_instance_check():
    ...


def test_pipe_typehint_provider_registration():
    ...


def test_pipe_singleton_per_type():
    ...


def test_pipe_dependency_injection():
    """Test if the pipe of the same type always returns the same object."""
    from beamlime.communication import Pipe
    from beamlime.complete_binders import PipeBinder
    from beamlime.constructors import Container, context_binder

    with context_binder(PipeBinder) as binder:
        pipe = binder[Pipe[int]]()
        assert pipe is binder[Pipe[int]]()
        assert Container[Pipe[int]] is pipe
