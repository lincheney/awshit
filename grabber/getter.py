import re
import sys
import math
from collections import defaultdict
from functools import cache
import boto3
import botocore.model
import utils
from typing import Self

@cache
def make_session():
    return boto3.session.Session(region_name='us-east-1')

def prepare_for_scoring(string: str):
    string = re.sub(r'([A-Z][a-z])', r' \1', string)
    string = re.sub(r'([a-z])([A-Z])', r'\1 \2', string)
    return [utils.singularise(x.lower()) for x in string.strip().split()]

class KeySpec(tuple[str]):
    ID_FORMATS = ('id', 'name', 'arn', 'code', 'list', 'identifier')
    SUFFIXES = ('key',)

    @classmethod
    def make(cls, key: str, method=None):
        method = [utils.singularise(x) for x in (method or '').lower().split('_')[1:]]
        return cls(method + prepare_for_scoring(key))

    def get_format(self):
        if self[-1] in self.ID_FORMATS:
            return self[-1]

    @staticmethod
    def key_score(length, correct_format=False, no_format=False, arn=False, suffix=False):
        return (
            length,
            int(correct_format),
            int(no_format),
            int(arn),
            int(not suffix),
        )

    @cache
    def matchers(self):
        max_len = len(self)
        if self.get_format():
            max_len -= 1

        matchers = []
        for l in range(max_len, 0, -1):
            for start in range(0, max_len - l + 1):
                k = ' ' + ' '.join(self[start:start+l]) + ' '

                score = self.key_score(l, correct_format=not self.get_format(), no_format=True)
                matchers.append((score, k))

                for id in self.ID_FORMATS:
                    score = self.key_score(l, correct_format=self.get_format() == id, arn=id == 'arn')
                    matchers.append((score, k + id + ' '))

        # add suffixes
        matchers += [((*s[:-1], 0), k + x + ' ') for (s, k) in matchers for x in self.SUFFIXES]

        # best scores first
        matchers.sort(reverse=True)
        return matchers

class Arg:
    pass

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
        if not self:
            return set()
        return set.union(*(v.call.args.used_methods() for k, v in self if isinstance(v, MethodCallOutput)))

    @cache
    def complexity_score(self):
        return 1 + sum(v.call.args.complexity_score() for k, v in self if isinstance(v, MethodCallOutput))

class OutputPath(tuple[str]):
    ITERATE = '*'

    def __repr__(self):
        return ".".join(self)

    @classmethod
    def from_shape(cls, shape):
        return list(cls().map_shape(shape))

    def append(self, val: str):
        return type(self)(self + (val,))

    def map_shape(self, shape, max_depth=10):
        if max_depth <= 0:
            return
        elif isinstance(shape, botocore.model.StructureShape):
            for k, v in shape.members.items():
                yield from self.append(k).map_shape(v, max_depth=max_depth-1)
        elif isinstance(shape, botocore.model.ListShape):
            yield from self.append(self.ITERATE).map_shape(shape.member, max_depth=max_depth-1)
        elif isinstance(shape, botocore.model.MapShape):
            yield from self.append(self.ITERATE).map_shape(shape.value, max_depth=max_depth-1)
        elif isinstance(shape, botocore.model.StringShape) or shape.type_name in ('integer', 'long', 'timestamp', 'float', 'double'):
            yield (self, shape)

    def non_branching(self):
        if self.ITERATE in self:
            return type(self)(x for x in self if x is not self.ITERATE)
        return self

    def for_scoring(self):
        return type(self)(prepare_for_scoring(' '.join(self.non_branching())))

    def matches(self, key: KeySpec):
        return any(k in self for k in key)

    def score(self, key: KeySpec):
        string = ' ' + ' '.join(self) + ' '
        for score, k in key.matchers():
            if string.endswith(k):
                return score


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
        return ['describe_instance_information']
        return self.client.meta.method_to_api_mapping.keys()

    @cache
    def make_method(self, method: str):
        return Method(method, self)

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
                if m.path.matches(key_spec):
                    bad_methods.append(m)
                else:
                    calls.extend(m.how_to_get(key, method=method, excluded_methods=excluded_methods, **kwargs))

        if not calls:
            for m in bad_methods:
                calls.extend(m.how_to_get(key, method=method, excluded_methods=excluded_methods, **kwargs))

        if calls:
            calls.sort(key=lambda c: c.score(key_spec), reverse=True)
        return calls

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

    def __repr__(self):
        return f'{self.service.name}.{self.name}'

    def how_to_call(self, args: Args|dict[str, Arg]=Args(), excluded_methods=frozenset()) -> Args|None:
        args = Args.make(args)

        for call, ex in self.cache.get(args, ()):
            if ex == excluded_methods:
                # exact match
                return call
            if call is not None and call.used_methods().isdisjoint(excluded_methods) and ex.issubset(excluded_methods):
                return call

        new_args = dict(args)
        for k, shape in self.requires.items():
            if k not in new_args:
                v = self.service.how_to_get(k, method=self.name, shape=shape, args=args, excluded_methods=excluded_methods | {self.name})
                if not v:
                    new_args = None
                    break
                new_args[k] = v[0]
        new_args = None if new_args is None else Args(new_args.items())
        self.cache[args].append((new_args, excluded_methods))
        return new_args

    def how_to_get(
        self,
        key: str,
        *,
        method: str|None=None,
        shape=None,
        args: Args|dict[str, Arg]=Args(),
        excluded_methods=frozenset(),
    ):
        if shape is not None:
            shape_args = Service.how_to_get_from_shape(shape)
            if shape_args is not None:
                yield from shape_args
                return

        if key in self.requires:
            # we also require this key
            return

        args = Args.make(args)
        spec = KeySpec.make(key, method)
        method_score = self.path.score(spec)
        new_args: Args | None = None
        for path, output_shape in OutputPath.from_shape(self.model.output_shape):
            path_score = OutputPath(self.path + path).for_scoring().score(spec)
            if path_score is None:
                continue
            if new_args is None:
                new_args = self.how_to_call(args, excluded_methods)
                if new_args is None:
                    return
            yield MethodCallOutput(self.call(new_args), path, method_score, path_score, shape)

    @cache
    def call(self, args: Args):
        return MethodCall(self, args)

class MethodCall(Arg):
    def __init__(self, method: Method, args: Args):
        self.method = method
        self.args = args

    def __repr__(self):
        return f'{self.method}({self.args})'

class MethodCallOutput(Arg):
    def __init__(self, call: MethodCall, output_path: OutputPath, method_score, path_score, shape):
        self.call = call
        self.output_path = output_path
        self.method_score = method_score or -math.inf
        self.path_score = path_score or -math.inf
        self.shape = shape

    def __repr__(self):
        return f'{self.call}.{self.output_path}'

    def score(self, key: KeySpec):
        return (
            # want better matching path
            self.path_score,
            # want better matching method
            self.method_score,
            # want fewer dependencies
            -self.call.args.complexity_score(),
            # want shorter method name
            -len(self.call.method.path),
            # want shorter path
            -len(self.output_path.non_branching()),
        )

if __name__ == '__main__':
    #  for c in Service('ec2').how_to_get('InstanceId'):
    #  for c in Service('ssm').how_to_get('Name', method='get_parameter_history'):
    for c in Service('ssm').how_to_get('instance'):
    #  shape = Service('lambda').make_method('create_function').model.input_shape.members['Runtime']
    #  for c in Service('lambda').how_to_get('Runtime', method='create_function', shape=shape):
        print(f'''DEBUG(tucson)\t{c = }''', file=sys.__stderr__)
        #  break
    #  print(Service('ec2').how_to_get('VpcId'))
    #  print(Service('elbv2').how_to_get('Attributes'))
    #  print(Service('ec2').how_to_get('associated enclave certificate iam roles CertificateArn'))
    #  for c in Service('dynamodb').how_to_get('Table'):
        #  print(f'''DEBUG(glut)  \t{c.score(KeySpec.from_str('Table')), c = }''', file=sys.__stderr__)
