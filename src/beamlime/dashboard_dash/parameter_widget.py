# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

from dash import dcc, html


def create_parameter_widget(param: dict) -> list:
    """Create appropriate widget based on parameter type."""
    param_type = param.get('param_type', 'STRING').upper()
    unit = param.get('unit')
    default_value = param.get('default', '')
    description = param.get('description', '')
    widget_id = {'type': 'param-input', 'name': param['name']}

    # Create label with tooltip for description
    label = html.Label(
        param['name'] if not unit else f'{param["name"]} [{unit}]',
        title=description,  # Tooltip on hover
        style={'cursor': 'help' if description else 'default'},
    )

    # Create appropriate input widget based on parameter type
    if param_type == 'BOOL':
        input_widget = dcc.Checklist(
            id=widget_id,
            options=[{'label': '', 'value': 'true'}],
            value=['true'] if default_value else [],
            style={'margin': '5px 0'},
        )
    elif param_type == 'INT':
        input_widget = dcc.Input(
            id=widget_id,
            type='number',
            step=1,
            value=default_value,
            style={'width': '100%'},
        )
    elif param_type == 'FLOAT':
        input_widget = dcc.Input(
            id=widget_id,
            type='number',
            step=0.1,
            value=default_value,
            style={'width': '100%'},
        )
    elif param_type == 'OPTIONS' and 'options' in param:
        options = [{'label': opt, 'value': opt} for opt in param['options']]
        input_widget = dcc.Dropdown(
            id=widget_id,
            options=options,
            value=default_value
            if default_value in param['options']
            else param['options'][0],
            style={'width': '100%'},
        )
    else:  # Default to string input for any other type
        input_widget = dcc.Input(
            id=widget_id,
            type='text',
            value=str(default_value),
            style={'width': '100%'},
        )

    # Add hidden div to store parameter type for value conversion
    param_type_store = html.Div(
        id={'type': 'param-type', 'name': param['name']},
        children=param_type,
        style={'display': 'none'},
    )

    return [
        label,
        input_widget,
        param_type_store,
        html.Div(style={'marginBottom': '10px'}),
    ]
