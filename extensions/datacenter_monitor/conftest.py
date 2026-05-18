"""Root conftest for the datacenter_monitor extension test suite.

Stubs out the datacenter_monitor_python package before pytest tries to import it,
preventing the Omniverse carb / Kit dependencies from being loaded in plain Python tests.
"""
import sys
import types
from pathlib import Path

_ROOT = Path(__file__).resolve().parent / "datacenter_monitor_python"

# Pre-register the package as a stub so pytest's importlib mode never executes
# the real __init__.py (which does `import carb` — an Omniverse-only runtime dep).
_pkg = "datacenter_monitor_python"
if _pkg not in sys.modules:
    stub = types.ModuleType(_pkg)
    stub.__path__ = [str(_ROOT)]
    stub.__package__ = _pkg
    stub.__spec__ = None
    sys.modules[_pkg] = stub

# Ensure config_loader (and future stdlib-only modules) are importable by path.
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
