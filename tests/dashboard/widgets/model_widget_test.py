# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from enum import Enum

import pydantic

from ess.livedata.dashboard.widgets.model_widget import ModelWidget


class TestModelWidget:
    def test_create_from_dynamic_model(self) -> None:
        def make_model(description: str, option: str) -> type[pydantic.BaseModel]:
            class Option(str, Enum):
                OPTION1 = option

            class InnerModel(pydantic.BaseModel):
                a: int = 1
                b: float = 2.0
                c: Option = Option.OPTION1

            class OuterModel(pydantic.BaseModel):
                inner: InnerModel = pydantic.Field(
                    default_factory=InnerModel, title='Title', description=description
                )

            return OuterModel

        widget = ModelWidget(make_model(description='abc', option='opt1'))
        valid, errors = widget.validate_parameters()
        assert valid
        assert not errors
