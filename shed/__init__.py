__version__ = '0.6.0'

try:
    from .simple_parallel import *
    from .translation_parallel import *
except ImportError:
    pass
from .simple import *
from .translation import *
