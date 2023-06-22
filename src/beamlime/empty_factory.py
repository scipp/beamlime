# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# Shared binders used for filling providers across the library.

from .constructors import Factory

empty_log_factory = Factory()
empty_pipe_factory = Factory()
