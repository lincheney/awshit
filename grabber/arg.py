from __future__ import annotations
from functools import cache
from typing import Self

class Arg:
    def unlazy(self) -> Self|None:
        return self

class StaticArg(Arg):
    def __init__(self, inner, shape):
        self.inner = inner
        self.shape = shape
    def __repr__(self):
        return repr(self.inner)

class Args(frozenset[tuple[str, Arg]]):
    def __str__(self):
        return ', '.join(f'{k}={v}' for k, v in self)

    @classmethod
    def make(cls, val: Self | dict[str, Arg]):
        if isinstance(val, dict):
            return cls(val.items())
        return val

    @cache
    def used_methods(self):
        from .method import MethodCallOutput
        if not self:
            return set()
        return set().union(*(v.call.args.used_methods() for k, v in self if isinstance(v, MethodCallOutput)))

    @cache
    def complexity_score(self):
        from .method import MethodCallOutput
        return 1 + sum((v.call.args.complexity_score() for k, v in self if isinstance(v, MethodCallOutput)), start=0)
