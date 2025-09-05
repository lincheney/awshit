from __future__ import annotations
import math
from collections import defaultdict
from functools import cache
from .service import Service
from .arg import Arg, Args
from .utils import OutputPath, KeySpec

class Method:
    def __init__(self, name: str, service: Service):
        self.service = service
        self.name = name
        self.model = self.service.client.meta.service_model.operation_model(self.service.client.meta.method_to_api_mapping[self.name])
        self.cache = defaultdict(list)
        self.path = OutputPath(self.name.split('_')[1:]).for_scoring()

        if self.model.input_shape and self.model.input_shape.required_members:
            self.requires = {k: self.model.input_shape.members[k] for k in self.model.input_shape.required_members}
        else:
            self.requires = {}
        self.requires_keys = {KeySpec.make(k).without_format() for k in self.requires}

    def __repr__(self):
        return f'{self.service.name}.{self.name}'

    def how_to_call(self, args: Args|dict[str, Arg]=Args(), excluded_methods=frozenset(), used_keys=frozenset()) -> Args|None:
        args = Args.make(args)

        for call, ex, uk in self.cache.get(args, ()):
            if ex == excluded_methods:
                # exact match
                return call
            if call is not None and call.used_methods().isdisjoint(excluded_methods) and ex.issubset(excluded_methods) and uk.issubset(used_keys):
                return call

        new_args = dict(args)
        for k, shape in self.requires.items():
            if k not in new_args:
                v = self.service.how_to_get(k, method=self.name, shape=shape, args=args, excluded_methods=excluded_methods | {self.name}, used_keys=used_keys)
                if not v:
                    new_args = None
                    break
                new_args[k] = v[0]
        new_args = None if new_args is None else Args(new_args.items())
        self.cache[args].append((new_args, excluded_methods, used_keys))
        return new_args

    def how_to_get(
        self,
        key: str,
        *,
        method: str|None=None,
        shape=None,
        args: Args|dict[str, Arg]=Args(),
        excluded_methods=frozenset(),
        used_keys=frozenset(),
    ):
        if shape is not None:
            shape_args = Service.how_to_get_from_shape(shape)
            if shape_args is not None:
                yield from shape_args
                return

        used_keys = used_keys | {KeySpec.make(key).without_format()}
        if used_keys & self.requires_keys:
            # we also require this key
            return

        args = Args.make(args)
        spec = KeySpec.make(key, method)
        method_score = spec.score(self.path)
        for path, output_shape in OutputPath.from_shape(self.model.output_shape):
            path_score = spec.score(OutputPath(self.path + path).for_scoring())
            if path_score is None:
                continue
            call = LazyMethodCall(self, args, excluded_methods, used_keys)
            yield LazyMethodCallOutput(call, path, method_score, path_score, shape)

class MethodCall(Arg):
    def __init__(self, method: Method, args: Args):
        self.method = method
        self.args = args

    def __repr__(self):
        return f'{self.method}({self.args})'

    def unlazy(self):
        unlazied = Args((k, a.unlazy()) for k, a in self.args)
        if None not in unlazied:
            if any(isinstance(a, LazyMethodCall) for a in self.args):
                self.args = unlazied
            return self

    def complexity_score(self):
        if self.args is None:
            return 1
        return self.args.complexity_score()

class LazyMethodCall(Arg):
    def __init__(self, method: Method, args: Args, excluded_methods, used_keys):
        self.method = method
        self.args = args
        self.excluded_methods = excluded_methods
        self.used_keys = used_keys

    def __repr__(self):
        return f'{self.method}(unknown!)'

    def unlazy(self):
        args = self.method.how_to_call(self.args, self.excluded_methods, self.used_keys)
        if args is not None:
            return MethodCall(self.method, args)

class BaseMethodCallOutput(Arg):
    def __init__(self, call: LazyMethodCall|MethodCall, output_path: OutputPath, method_score, path_score, shape):
        self.call = call
        self.output_path = output_path
        self.method_score = method_score or (-math.inf,)
        self.path_score = path_score or (-math.inf,)
        self.shape = shape

    def __repr__(self):
        return f'{self.call}.{self.output_path}'

    def quick_score(self, key: KeySpec):
        return (
            # want better matching path
            self.path_score,
            # want better matching method
            self.method_score,
        )

    def unlazy(self):
        return self

class LazyMethodCallOutput(BaseMethodCallOutput):
    def __init__(self, call: LazyMethodCall, *args, **kwargs):
        super().__init__(call, *args, **kwargs)

    def unlazy(self):
        call = self.call.unlazy()
        if call is not None:
            return MethodCallOutput(call, self.output_path, self.method_score, self.path_score, self.shape)

class MethodCallOutput(BaseMethodCallOutput):
    def __init__(self, call: MethodCall, *args, **kwargs):
        super().__init__(call, *args, **kwargs)

    def score(self, key: KeySpec):
        return self.quick_score(key) + (
            # want fewer dependencies
            -self.call.complexity_score(),
            # want shorter method name
            -len(self.call.method.path),
            # want shorter path
            -len(self.output_path.non_branching()),
        )
