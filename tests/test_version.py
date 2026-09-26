"""The package must report the version it was released as.

``pyproject.toml`` is the one place the version is written. Until v0.3.3,
``eskom_grid.__version__`` was a second, hand-typed copy; every release bumped
the first and forgot the second, so v0.3.2 reported itself as 0.1.0. The cloud
handlers log this value to show which code a run used, so a wrong one is worse
than none: it looks trustworthy.
"""

from importlib.metadata import version

import eskom_grid


def test_version_matches_the_installed_package():
    assert eskom_grid.__version__ == version("eskom-grid")
