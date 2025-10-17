import re
from functools import cache
import botocore.model

def singularise(string) -> str:
    if string.endswith('ies'):
        return string.removesuffix('ies') + 'y'
    if string.endswith('aliases'):
        return string.removesuffix('aliases') + 'alias'
    if string.endswith('indices'):
        return string.removesuffix('indices') + 'index'
    if re.search('(address|prefix|patch)es', string):
        return string.removesuffix('es')
    if string.endswith('s') and not any(string.endswith(s) for s in ('ss', 'bus', 'status', 'alias', 'analysis')):
        return string.removesuffix('s')
    if string.endswith('i') and not any(string.endswith(s) for s in ('api',)):
        return string.removesuffix('i') + 'us'
    return string

def prepare_for_scoring(string: str):
    string = re.sub(r'([A-Z][a-z])', r' \1', string)
    string = re.sub(r'([a-z])([A-Z])', r'\1 \2', string)
    return [singularise(x.lower()) for x in re.split(r'[-_.\s]+', string.strip())]

class KeySpec(tuple[str]):
    ID_FORMATS = ('id', 'name', 'arn', 'code', 'list', 'identifier')
    SUFFIXES = ('key',)

    @classmethod
    def make(cls, key: str, method=None):
        method = [singularise(x) for x in (method or '').lower().split('_')[1:]]
        return cls(method + prepare_for_scoring(key))

    def get_format(self):
        if self[-1] in self.ID_FORMATS:
            return self[-1]

    def without_format(self):
        if self.get_format():
            return self[:-1]
        return self

    @staticmethod
    def key_score(length, correct_format=False, no_format=False, arn=False, suffix=False):
        return (
            length,
            int(correct_format),
            # int(no_format),
            # int(arn),
            int(not suffix),
        )

    @cache
    def matchers(self):
        max_len = len(self.without_format())
        matchers = []
        for l in range(max_len, -1, -1):
            for start in range(0, max_len - l + 1):
                k = ' ' + ' '.join(self[start:start+l]) + ' ' if l else ' '

                if l:
                    score = self.key_score(l, no_format=True)
                    matchers.append((score, k))

                for id in self.ID_FORMATS:
                    score = self.key_score(l, correct_format=self.get_format() == id, arn=id == 'arn')
                    matchers.append((score, k + id + ' '))

        # add suffixes
        matchers += [((*s[:-1], 0), k + x + ' ') for (s, k) in matchers for x in self.SUFFIXES]

        # best scores first
        matchers.sort(reverse=True)
        return matchers

    def matches(self, items):
        return any(k in items for k in self.without_format())

    def score(self, items):
        string = ' ' + ' '.join(items) + ' '
        for score, k in self.matchers():
            if string.endswith(k):
                return score

class OutputPath(tuple[str]):
    ITERATE = '*'

    def __repr__(self):
        return ".".join(self)

    @classmethod
    def from_shape(cls, shape):
        return list(cls().map_shape(shape))

    def append(self, val: str):
        return type(self)(self + (val,))

    def map_shape(self, shape, parent=None, max_depth=10, only_leaves=True):
        if not shape or max_depth <= 0:
            return
        if not only_leaves:
            yield (self, shape, parent)
        if isinstance(shape, botocore.model.StructureShape):
            for k, v in shape.members.items():
                yield from self.append(k).map_shape(v, shape, max_depth=max_depth-1, only_leaves=only_leaves)
        elif isinstance(shape, botocore.model.ListShape):
            yield from self.append(self.ITERATE).map_shape(shape.member, shape, max_depth=max_depth-1, only_leaves=only_leaves)
        elif isinstance(shape, botocore.model.MapShape):
            yield from self.append(self.ITERATE).map_shape(shape.value, shape, max_depth=max_depth-1, only_leaves=only_leaves)
        elif only_leaves and (isinstance(shape, botocore.model.StringShape) or shape.type_name in ('integer', 'long', 'timestamp', 'float', 'double')):
            yield (self, shape, parent)

    def non_branching(self):
        if self.ITERATE in self:
            return type(self)(x for x in self if x is not self.ITERATE)
        return self

    def for_scoring(self):
        return type(self)(prepare_for_scoring(' '.join(self.non_branching())))

    def to_jq(self):
        return ''.join('[]' if x is self.ITERATE else '.'+x for x in self)

    def to_jmespath(self):
        return self.to_jq().removeprefix('.')

    def apply_to(self, data):
        data = [data]
        for k in self:
            if k is self.ITERATE:
                data = [y for x in data for y in x]
            else:
                data = [x[k] for x in data]
        return data
