import beamlime.dashboard  # noqa: F401


def test_holoview_unit_format_same_as_plopp():
    import holoviews as hv

    assert hv.Dimension.unit_format == ' [{unit}]'
