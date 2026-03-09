"""Pytest setup for CLP package tests."""

# Make CLP package fixtures available to package tests without imports.
pytest_plugins = [
    "tests.clp_package_tests.clp_package_fixtures",
]
