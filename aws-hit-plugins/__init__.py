import sys
import pkgutil

def awscli_initialize(event_hooks):
    current_module = sys.modules[__name__]
    for sub in pkgutil.iter_modules(current_module.__path__):
        sub = __import__(current_module.__name__ + '.' + sub.name, fromlist=[sub.name])
        sub.awscli_initialize(event_hooks)
