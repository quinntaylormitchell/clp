"""Pytest setup for CLP binary tests."""

# Make CLP binary fixtures available to binary tests without imports.
pytest_plugins = [
    "clp_binary_tests.integration_test_logs",
    "clp_binary_tests.clp_binary_path_configs",
]
