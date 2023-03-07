from enum import Enum


DEFAULT_CONFIG_PATH="~/.data-peek/config.yaml"


class NewDataHandleOptions(Enum):
    ADD = "ADD"
    REPLACE = "REPLACE"
    APPEND = "APPEND"
    SKIP = "SKIP"  # When the target data is needed to be processed only once


class StreamChannelOptions(Enum):
    KAFKA = "KAFKA"
    FILEIO = "FILEIO"
    STDIO = "STDIO"

