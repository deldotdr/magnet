
# This library has a minimum required python version
import sys
if not hasattr(sys, "version_info") or sys.version_info < (2,5):
    raise RuntimeError("Magnet requires Python 2.5 or later.")
del sys

from magnet._version import version
__version__ = version.short()
