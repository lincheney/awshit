import re

def singularise(string) -> str:
    if string.endswith('ies'):
        return string.removesuffix('ies') + 'y'
    if string.endswith('aliases'):
        return string.removesuffix('aliases') + 'alias'
    if string.endswith('indices'):
        return string.removesuffix('indices') + 'index'
    if re.search('(address|prefix)es', string):
        return string.removesuffix('es')
    if string.endswith('s') and not any(string.endswith(s) for s in ('ss', 'bus', 'status', 'alias', 'analysis')):
        return string.removesuffix('s')
    if string.endswith('i') and not any(string.endswith(s) for s in ('api',)):
        return string.removesuffix('i') + 'us'
    return string
