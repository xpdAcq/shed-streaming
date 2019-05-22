__version__ = '0.7.2'

try:
    from .simple_parallel import *
    from .translation_parallel import *
except ImportError:
    pass
from .simple import *
from .translation import *
