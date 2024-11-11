"""
This config is used during Live Data Reduction detector view.
Currently the instrument specific config is stored in python files, but
they can be moved to a separate file format in the future.
"""

from .dream import _dream
from .loki import _loki
from .nmx import _nmx

__all__ = ["_dream", "_loki", "_nmx"]
