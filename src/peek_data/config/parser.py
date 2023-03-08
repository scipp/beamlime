import yaml
from .resources import load_default_config_yaml, load_user_config_yaml


class YamlDict(dict):
    def __init__(self, mapping: dict={}, **kwargs) -> None:
        super().__init__(self, mapping, **kwargs)
    
    def to_yaml(self) -> str:
        yaml.dump(super())

    def update_from_yaml(self, raw_yaml: str):
        self.update(yaml.load(raw_yaml))


class PeekingConfig(YamlDict):
    def __init__(self, mapping: dict={}, **kwargs) -> None:
        super().__init__(self, mapping, **kwargs)


class _Config(PeekingConfig):
    def __init__(self, mapping: dict = {}, **kwargs) -> None:
        super().__init__(mapping, **kwargs)

    @property
    def dashbord_config(self) -> PeekingConfig:
        return self.get('dashboard')
    
    @property
    def data_stream_config(self) -> PeekingConfig:
        return self.get('data-stream')
    
    @property
    def data_reduction_config(self) -> PeekingConfig:
        return self.get('data-reduction')


class DefaultConfig(_Config):
    def __init__(self) -> None:
        super().__init__()
        self.update_from_yaml(load_default_config_yaml())


class UserConfig(_Config):
    def __init__(self, config_path: str=None, mapping: dict={}, **kwargs) -> None:
        super().__init__(mapping, **kwargs)
        if config_path is not None:
            self.update_from_yaml(load_user_config_yaml(path=config_path))

