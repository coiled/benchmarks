import pytest


def pytest_collection_modifyitems(config, items):
    # Ensure stability tests use spot instances by default
    marker = pytest.mark.backend_options(spot=True)
    for item in items:
        # Add a module-level `spot=True` backend option marker if one doesn't already exist
        module = item.parent
        if not any(
            m.kwargs.get("spot", False)
            for m in module.iter_markers(name="backend_options")
        ):
            module.add_marker(marker)
