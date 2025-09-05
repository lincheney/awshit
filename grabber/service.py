from __future__ import annotations
import re
import itertools
from functools import cache
import boto3
import botocore.model

@cache
def make_session():
    return boto3.session.Session(region_name='us-east-1')

class Service:
    def __init__(self, name: str, session=None):
        if session is None:
            session = make_session()
        self.session = session
        self.name = name
        self.client = session.client(self.name)

    def __repr__(self):
        return self.name

    def get_method_names(self) -> list[str]:
        return self.client.meta.method_to_api_mapping.keys()

    @cache
    def make_method(self, method: str):
        return Method(method, self)

    def unlazy_calls(self, calls, key_spec):
        # group and sort by the quick score
        group_key = lambda c: c.quick_score(key_spec)
        for score, group in itertools.groupby(sorted(calls, key=group_key, reverse=True), group_key):
            # then unlazy and get the real score
            if group := list(filter(None, (c.unlazy() for c in group))):
                group.sort(key=lambda c: c.score(key_spec), reverse=True)
                yield group

    @classmethod
    def how_to_get_from_shape(cls, shape):
        if isinstance(shape, botocore.model.ListShape):
            return cls.how_to_get_from_shape(shape.member)
        if getattr(shape, 'enum', None):
            return [StaticArg(x, shape) for x in shape.enum]

    def how_to_get(self, key: str, *, method: str|None=None, shape=None, excluded_methods=frozenset(), **kwargs):
        if shape is not None:
            args = self.how_to_get_from_shape(shape)
            if args is not None:
                return args

        key_spec = KeySpec.make(key, method)
        calls = []
        bad_methods = []

        for m in self.get_method_names():
            if m not in excluded_methods and re.match('(list|describe|get)', m):
                m = self.make_method(m)
                if key_spec.matches(m.path):
                    calls.extend(m.how_to_get(key, method=method, excluded_methods=excluded_methods, **kwargs))
                else:
                    bad_methods.append(m)

        for group in self.unlazy_calls(calls, key_spec):
            return group

        for m in bad_methods:
            calls.extend(m.how_to_get(key, method=method, excluded_methods=excluded_methods, **kwargs))

        for group in self.unlazy_calls(calls, key_spec):
            return group

        return []

from .method import Method
from .arg import StaticArg
from .utils import KeySpec
