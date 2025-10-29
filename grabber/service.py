from __future__ import annotations
import re
import itertools
from functools import cache
import botocore.model
import botocore.session
import botocore.exceptions

@cache
def make_session():
    return botocore.session.Session()

class Service:
    def __init__(self, name: str, session=None):
        if session is None:
            session = make_session()
        self.session = session
        self.name = name
        try:
            self.client = session.create_client(self.name)
        except botocore.exceptions.NoRegionError:
            self.client = session.create_client(self.name, region_name='us-east-1')

    def __repr__(self):
        return self.name

    def get_method_names(self) -> list[str]:
        return self.client.meta.method_to_api_mapping.keys()

    @cache
    def make_method(self, method: str):
        return Method(method, self)

    def sort_calls(self, calls, key_spec, scorer):
        # group and sort by the quick score
        group_key = lambda c: scorer(c, key_spec)
        for score, group in itertools.groupby(sorted(calls, key=group_key, reverse=True), group_key):
            group = list(group)
            # then unlazy and get the real score
            if resolved := list(filter(None, (c.unlazy() for c in group))):
                resolved.sort(key=lambda c: c.score(key_spec), reverse=True)
                yield resolved

    @classmethod
    def how_to_get_from_shape(cls, shape):
        if isinstance(shape, botocore.model.ListShape):
            return cls.how_to_get_from_shape(shape.member)
        if getattr(shape, 'enum', None):
            return [MultiArg(shape.enum)]

    def how_to_get(self, key: str, *, method: str|None=None, shape=None, excluded_methods=frozenset(), **kwargs):
        if shape is not None:
            args = self.how_to_get_from_shape(shape)
            if args is not None:
                return args

        key_spec = KeySpec.make(key)
        method_key_spec = KeySpec.make(key, method)
        best_methods = []
        best_method_methods = []
        good_methods = []
        bad_methods = []

        for m in self.get_method_names():
            if m not in excluded_methods and re.match('(list|describe|get)', m):
                meth = self.make_method(m)
                if meth.path in [tuple(k.split()) for s, k in key_spec.matchers()]:
                    best_methods.append(meth)
                elif meth.path in [tuple(k.split()) for s, k in method_key_spec.matchers()]:
                    best_method_methods.append(meth)
                elif key_spec.matches(meth.path):
                    good_methods.append(meth)
                else:
                    bad_methods.append(meth)

        for methods, scorer in [
            (best_methods, lambda c, k: (-len(c.call.method.requires), c.quick_score(k))),
            (best_method_methods, lambda c, k: (-len(c.call.method.requires), c.quick_score(k))),
            (good_methods, lambda c, k: c.quick_score(k)),
            (bad_methods,  lambda c, k: c.quick_score(k)),
        ]:
            calls = []
            for m in methods:
                calls.extend(m.how_to_get(key, method=method, excluded_methods=excluded_methods, **kwargs))
            for group in self.sort_calls(calls, key_spec, scorer):
                return group

        return []

from .method import Method
from .arg import MultiArg
from .utils import KeySpec
