import sys
from pathlib import Path

# Make config_loader importable in tests without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
