#!/usr/bin/env python3

import sys
import re
import os
import shlex
import math
import itertools
import html
import botocore.model
import botocore.session
import awscli.alias
import awscli.clidriver
import awscli.customizations.waiters
import awscli.formatter
import awscli.autocomplete.main as autocomplete
from .grabber import utils as grabber_utils
from .grabber import service as grabber_service

def awscli_initialize(event_hooks):
    event_hooks.register('building-command-table.main', inject_commands)

def inject_commands(command_table, session, command_object, **kwargs):
    command_table['.tab-completion'] = lambda *args: Completer().complete(command_object, *args)

class CommandLine:
    def __init__(self, inner):
        self.inner = inner
    def __getitem__(self, key):
        return ''
        #  return ' '.join(self.inner)[key]
    def split(self):
        return list(self.inner)

def extract_doc(doc):
    doc = re.sub(r'\n\s*', ' ', doc, flags=re.M)
    if match := re.search(r'<p>(.*?)(\. |</p>)', doc):
        doc = match.group(1).strip()
    elif re.match(r'\s*<p\s*/>\s*$', doc):
        doc = ''
    else:
        doc = doc.partition('. ')[0]
    return doc

def remove_xml(string):
    return html.unescape(re.sub(r'<.*?>', '', string)).strip()

class Completer:

    def complete_from_completer(self, completer):
        results = completer.complete(self.parsed)
        return results and [(item.name, item.help_text) for item in results]

    def complete_from_subcommand_table(self, cli):
        # this is the slowest completer, so try as many shortcuts as possible
        seen = set()

        if cli is self.driver:
            # use the cache only if driver
            index = autocomplete.model.ModelIndex()
            for name, doc in self.complete_from_completer(autocomplete.basic.ModelIndexCompleter(index)) or ():
                if doc:
                    seen.add(name)
                    yield name, doc

        for name, cmd in cli.subcommand_table.items():
            # its worth filtering by current_fragment here because getting the help doc is expensive
            if name in seen or not name.startswith(self.parsed.current_fragment):
                continue

            doc = ''
            if not self.need_docs:
                pass
            elif isinstance(cmd, awscli.alias.ServiceAliasCommand):
                doc = f'(alias) aws {cmd._alias_value} ...'
            elif isinstance(cmd, awscli.alias.BaseAliasCommand):
                doc = f'(alias) {cmd._alias_value}'
            elif isinstance(cmd, awscli.customizations.waiters.WaiterStateCommand):
                doc = cmd.DESCRIPTION
            elif isinstance(cmd, awscli.clidriver.ServiceOperation):
                doc = cmd._operation_model.documentation
            elif hasattr(cmd, 'service_model'):
                doc = cmd.service_model.service_id
            elif hasattr(cmd, 'create_help_command') and (help := cmd.create_help_command()):
                if cli is self.driver:
                    doc = help.description.split('.')[0].strip()
                elif hasattr(help, 'description'):
                    doc = help.description
                elif hasattr(help, 'obj') and hasattr(help.obj, 'documentation'):
                    doc = help.obj.documentation
                else:
                    doc = help.doc.getvalue().decode('utf8')
            yield name, remove_xml(extract_doc(doc))

    @staticmethod
    def get_args_from_arg_table(cli, required, positional, exclude):
        for v in cli.arg_table.values():
            if bool(v.required) == required and bool(v.cli_name.startswith('-')) != positional and v.name not in exclude:
                yield v

    def complete_from_arg_table_positional(self, cli, required):
        exclude = self.parsed.parsed_params | self.parsed.global_params
        positional = self.get_args_from_arg_table(cli, required, True, exclude)
        if files := [v for v in positional if v.argument_model.name in {'StreamingOutputArgument'}]:
            v = files[0]
            doc = remove_xml(extract_doc(v.documentation))
            self.complete_files(self.parsed.current_fragment, doc=f'({v.name}) {doc}')

    def complete_from_arg_table(self, cli, required):
        exclude = self.parsed.parsed_params | self.parsed.global_params
        for v in self.get_args_from_arg_table(cli, required, False, exclude):
            doc = remove_xml(extract_doc(v.documentation))
            if required:
                doc = '(required) ' + doc
            yield v.cli_name, doc

    def complete_from_shape(self, shape, arg, seen=()):
        if type(shape) in seen:
            return
        seen += (type(shape),)
        if isinstance(shape, botocore.model.MapShape):
            yield ('key=?,value=?', '')
        elif isinstance(shape, botocore.model.StringShape):
            if shape.enum:
                yield from ((i, '') for i in shape.enum)
            else:
                results = {k: '' for k in getattr(arg, 'choices', ()) or ()}
                if doc := re.search(r'<ul>(.*?)</ul>', getattr(arg, '_help', '')):
                    for match in re.findall(r'<li>(.*?)</li>', doc.group(1)):
                        match, _, descr = match.strip().partition(' - ')
                        results[match] = descr
                yield from results.items()
        elif isinstance(shape, botocore.model.ListShape):
            yield from self.complete_from_shape(shape.member, arg, seen=seen)
        elif isinstance(shape, botocore.model.StructureShape):
            values = [list(self.complete_from_shape(m, None, seen=seen)) or [('', '')] for m in shape.members.values()]
            if arg and values == [[('', '')], [('', '')]]:
                # no values, grab from doc
                if doc := re.search(r'<ul>(.*?)</ul>', arg.argument_model.documentation):
                    if matches := re.findall(r'<p>(.*?)</p>', doc.group(1)):
                        for match in matches:
                            match, _, descr = match.strip().partition(' - ')
                            values = re.search(r'\((.+)\)', descr)
                            values = values and re.findall(r'<code>(.*?)</code>', values.group(1))
                            if values:
                                descr = re.sub(r'\s*\(.+\)\s*', '', descr)
                                values = [v.strip() for v in values]

                            for v in values or ['']:
                                result = ','.join(k+'='+v for k, v in zip(shape.members, [remove_xml(match), v]))
                                yield (result, remove_xml(descr))
                        return

            for combo in itertools.product(*values):
                result = ','.join(k+'='+v[0] for k, v in zip(shape.members, combo))
                display = next((v[1] for v in combo if v[1]), '')
                yield (result, display)

    def complete_regions(self):
        regions = {k: v['description'] for p in self.driver.session.get_data('endpoints')['partitions'] for k, v in p['regions'].items()}
        for name, doc in self.complete_from_completer(autocomplete.basic.RegionCompleter()) or ():
            yield (name, regions[name])

    def complete_files(self, fragment, doc='', prefix=''):
        dirname = os.path.dirname(fragment)
        basename = os.path.basename(fragment)

        root, dirs, files = next(os.walk(dirname or '.'))
        if not basename.startswith('.'):
            dirs = [d for d in dirs if not d.startswith('.')]
            files = [f for f in files if not f.startswith('.')]
        self.print_completions([(prefix + os.path.join(dirname, x) + '/', doc) for x in dirs], suffix='')
        self.print_completions([(prefix + os.path.join(dirname, x), doc) for x in files])

    def print_completions(self, results, suffix=None):
        if results := list(results):
            names, docs = zip(*results)
            docs = names
            if self.need_docs:
                self.max_name_len = max(self.max_name_len, max(map(len, names)))
                width = math.ceil(self.max_name_len / 4 + 1) * 4
                if any(docs):
                    fmt = f'%-{width}s%s'
                    docs = [fmt % (n, d) for n, d in results]

            names = [shlex.quote(n) + (' ' if suffix is None else '') for n in names]
            print('\n'.join(['complete'] + names + list(docs)), end='\x00')


    def complete(self, driver, argv, opts=None):
        shell, *argv = argv
        self.driver = driver
        self.current = argv[-1]
        self.need_docs = shell == 'zsh'
        self.parsed = None
        self.max_name_len = 0

        completer = autocomplete.create_autocompleter(driver=driver)
        # we've already presplit the command
        argv = CommandLine(argv)

        while True:
            parsed = completer._parser.parse(argv)

            commands = parsed.lineage[1:]
            if parsed.lineage and parsed.current_command and parsed.current_fragment is not None:
                commands.append(parsed.current_command)
            if not commands and parsed.unparsed_items:
                if parsed.unparsed_items[0] in driver.subcommand_table:
                    commands.append(parsed.unparsed_items.pop(0))

            cli = driver
            for c in commands:
                cli = cli.subcommand_table[c]

            if isinstance(cli, awscli.alias.ServiceAliasCommand):
                argv = f'aws {cli._alias_value} '
            elif isinstance(cli, awscli.alias.ExternalAliasCommand) and re.match(r'!(:;)?\w+\(\)[({]#', re.sub(r'\s+', '', cli._alias_value)):
                alias = cli._alias_value.splitlines()[0].partition('#')[2].strip()
                argv = f'aws {alias} '
            elif isinstance(cli, awscli.alias.BaseAliasCommand):
                # no idea how to complete
                return
            else:
                break

        # positional multi args are not handled well
        if parsed.current_param is None and parsed.parsed_params:
            param = cli.arg_table[list(parsed.parsed_params)[-1]]
            nargs = param.nargs
            if param.positional_arg and nargs > sum(1 for x in parsed.unparsed_items if not x.startswith('-')) + 1:
                parsed.current_param = param.name
        self.parsed = parsed

        session = botocore.session.get_session()
        if 'profile' in parsed.global_params:
            session.set_config_variable('profile', parsed.global_params['profile'])
        if 'region' in parsed.global_params:
            session.set_config_variable('region', parsed.global_params['region'])

        if not parsed.current_fragment:
            parsed.current_fragment = self.current

        if parsed.current_param == 'region':
            self.print_completions(self.complete_regions())

        elif parsed.current_param == 'output':
            self.print_completions((k, (v.__doc__ or '').strip().split('\n')[0]) for k, v in awscli.formatter.CLI_OUTPUT_FORMATS.items())

        elif parsed.current_param == 'profile':
            self.print_completions(self.complete_from_completer(autocomplete.basic.ProfileCompleter()) or ())

        elif parsed.current_param == 'query' and cli is not driver:
            shapes = grabber_utils.OutputPath().map_shape(cli._operation_model.output_shape, only_leaves=False)
            shapes = ((path.to_jmespath(), remove_xml(shape.documentation or getattr(parent, 'documentation', ''))) for path, shape, parent in shapes if path)
            self.print_completions(shapes)

        elif parsed.current_param in {'cli-input-json', 'cli-input-yaml'}:
            self.complete_files(parsed.current_fragment)

        elif [*commands, parsed.current_param] == ['cloudformation', 'deploy', 'template-file']:
            self.complete_files(parsed.current_fragment)

        elif match := re.match('file://|fileb://', parsed.current_fragment):
            self.complete_files(parsed.current_fragment[match.end():], prefix=match.group(0))

        elif (param := cli.arg_table.get(parsed.current_param)) and cli is not driver:

            if matches := list(self.complete_from_shape(param.argument_model, param)):
                self.print_completions(matches)

            elif commands[0] == 's3' and commands[1] not in {'mb', 'rb'} and param.name == 'paths' and not param.argument_model:
                # complete local files
                if param._default != 's3://':
                    doc = remove_xml(extract_doc(param.documentation))
                    self.complete_files(parsed.current_fragment, doc=doc)

                if parsed.current_fragment.startswith('s3://'):
                    # have some s3 path already
                    bucket, slash, prefix = parsed.current_fragment.removeprefix('s3://').removesuffix('*').partition('/')
                    recursive = parsed.current_fragment.endswith('*')

                    if slash:
                        kwargs = dict(Bucket=bucket, Prefix=prefix)
                        if not recursive:
                            kwargs['Delimiter'] = '/'

                        for page in session.create_client('s3').get_paginator('list_objects_v2').paginate(**kwargs):
                            self.print_completions([(f's3://{bucket}/{x["Prefix"]}', '') for x in page.get('CommonPrefixes', ())], suffix='')
                            self.print_completions((f's3://{bucket}/{x["Key"]}', '') for x in page.get('Contents', ()))
                        return

                if 's3://'.startswith(parsed.current_fragment[:len('s3://')]):
                    # grab some buckets
                    for page in session.create_client('s3').get_paginator('list_buckets').paginate():
                        self.print_completions([(f's3://{x["Name"]}/', '') for x in page.get('Buckets', ())], suffix='')

            # probably a file
            elif param.cli_name.endswith('-outfile') and isinstance(param.argument_model, botocore.model.StringShape):
                doc = remove_xml(extract_doc(param.documentation))
                self.complete_files(parsed.current_fragment, doc=doc)

            else:
                # uhhh the builtin server side compelter is not that great sometimes
                # e.g. trying to complete "aws ssm get-parameter --name ..." it fetches ssm doc names instead of ssm param names
                #  results = complete_from_completer(parsed, autocomplete.serverside.create_server_side_completer(None))
                results = None
                if results is None:
                    # drop the verb
                    info = commands[1].partition('-')[2]
                    if results := grabber_service.Service(commands[0], session).how_to_get(parsed.current_param, method=info):
                        # use the best one
                        print('Running:', results[0], file=sys.stderr)
                        for page in results[0].execute({}):
                            self.print_completions((x, '') for x in page)
                else:
                    self.print_completions(results)

        if not parsed.current_param or parsed.parsed_params.get(parsed.current_param):
            # preformat
            self.max_name_len = max(
                self.max_name_len,
                max(map(len, cli.subcommand_table), default=0),
                max((len(v.cli_name) for v in cli.arg_table.values()), default=0),
                max((len(v.cli_name) for v in driver.arg_table.values()), default=0),
            )

            self.print_completions(self.complete_from_subcommand_table(cli))
            for required in (True, False):
                self.print_completions(self.complete_from_arg_table(cli, required))
                self.complete_from_arg_table_positional(cli, required)
                if cli is not driver:
                    self.print_completions(self.complete_from_arg_table(driver, required))
