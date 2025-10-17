#!/usr/bin/env python3

import re
import os
import shlex
import math
from functools import partial
import itertools
import html
import botocore.model
import awscli.alias
import awscli.formatter
import awscli.autocomplete.main as autocomplete
from .grabber import utils as grabber_utils
from .grabber import service as grabber_service

def awscli_initialize(event_hooks):
    event_hooks.register('building-command-table.main', inject_commands)

def inject_commands(command_table, session, command_object, **kwargs):
    command_table['.tab-completion'] = partial(completion, command_object)

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
    if '<p>' in doc:
        doc = re.search(r'<p>(.*?)(\. |</p>)', doc).group(1).strip()
    elif re.match(r'\s*<p\s*/>\s*$', doc):
        doc = ''
    else:
        doc = doc.partition('. ')[0]
    return doc

def remove_xml(string):
    return html.unescape(re.sub(r'<.*?>', '', string)).strip()

def complete_from_completer(parsed, completer):
    results = completer.complete(parsed)
    return results and [(item.name, item.help_text) for item in results]

def complete_from_subcommand_table(parsed, cli, driver):
    seen = set()

    if cli is driver:
        # use the cache only if driver
        index = autocomplete.model.ModelIndex()
        for name, doc in complete_from_completer(parsed, autocomplete.basic.ModelIndexCompleter(index)) or ():
            if doc:
                seen.add(doc)
                yield name, doc

    for name, cmd in cli.subcommand_table.items():
        # its worth filtering by current_fragment here because getting the help doc is expensive
        if name in seen or not name.startswith(parsed.current_fragment):
            continue

        doc = ''
        if isinstance(cmd, awscli.alias.ServiceAliasCommand):
            doc = f'(alias) aws {cmd._alias_value} ...'
        elif isinstance(cmd, awscli.alias.BaseAliasCommand):
            doc = f'(alias) {cmd._alias_value}'
        elif hasattr(cmd, 'service_model'):
            doc = cmd.service_model.service_id
        elif hasattr(cmd, 'create_help_command') and (help := cmd.create_help_command()):
            if cli is driver:
                doc = help.description.split('.')[0].strip()
            elif hasattr(help, 'description'):
                doc = help.description
            elif hasattr(help, 'obj') and hasattr(help.obj, 'documentation'):
                doc = help.obj.documentation
            else:
                doc = help.doc.getvalue().decode('utf8')
        yield name, remove_xml(extract_doc(doc))

def get_args_from_arg_table(cli, required, positional, exclude):
    for v in cli.arg_table.values():
        if bool(v.required) == required and bool(v.cli_name.startswith('-')) != positional and v.name not in exclude:
            yield v

def complete_from_arg_table_positional(parsed, cli, required):
    exclude = parsed.parsed_params | parsed.global_params
    positional = get_args_from_arg_table(cli, required, True, exclude)
    if files := [v for v in positional if v.argument_model.name in {'StreamingOutputArgument'}]:
        v = files[0]
        doc = remove_xml(extract_doc(v.documentation))
        complete_files(parsed.current_fragment, doc=f'({v.name}) {doc}')

def complete_from_arg_table(parsed, cli, required):
    exclude = parsed.parsed_params | parsed.global_params
    for v in get_args_from_arg_table(cli, required, False, exclude):
        doc = remove_xml(extract_doc(v.documentation))
        if required:
            doc = '(required) ' + doc
        yield v.cli_name, doc

def complete_from_shape(shape, arg, seen=()):
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
        yield from complete_from_shape(shape.member, arg, seen=seen)
    elif isinstance(shape, botocore.model.StructureShape):
        values = [list(complete_from_shape(m, None, seen=seen)) or [('', '')] for m in shape.members.values()]
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

def complete_regions(parsed, driver):
    regions = {k: v['description'] for p in driver.session.get_data('endpoints')['partitions'] for k, v in p['regions'].items()}
    for name, doc in complete_from_completer(autocomplete.basic.RegionCompleter(), parsed) or ():
        yield (name, regions[name])

def complete_files(fragment, doc='', prefix=''):
    dirname = os.path.dirname(fragment)
    basename = os.path.basename(fragment)

    for root, dirs, files in os.walk(dirname or '.'):
        if not basename.startswith('.'):
            dirs = [d for d in dirs if not d.startswith('.')]
            files = [f for f in files if not f.startswith('.')]
        print_completions([(prefix + os.path.join(dirname, x) + '/', doc) for x in dirs], suffix='')
        print_completions([(prefix + os.path.join(dirname, x), doc) for x in files])
        break

max_name_len = 0
def print_completions(results, suffix=None):
    global max_name_len
    if results := list(results):
        names = [n for n, d in results]
        max_name_len = max(max_name_len, math.ceil(max(map(len, names)) / 4 + 1) * 4)
        fmt = f'%-{max_name_len}s%s'
        docs = [fmt % (n, d) for n, d in results]
        names = [shlex.quote(n) + (' ' if suffix is None else '') for n in names]
        print('\n'.join(['complete', suffix or ''] + names + docs), end='\x00')

def completion(driver, argv, opts=None):
    current = argv[-1]
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

    if not parsed.current_fragment:
        parsed.current_fragment = current

    if parsed.current_param == 'region':
        print_completions(complete_regions(parsed, driver))

    elif parsed.current_param == 'output':
        print_completions((k, '') for k in awscli.formatter.CLI_OUTPUT_FORMATS)

    elif parsed.current_param == 'profile':
        print_completions(complete_from_completer(parsed, autocomplete.basic.ProfileCompleter()) or ())

    elif parsed.current_param == 'query' and cli is not driver:
        shapes = grabber_utils.OutputPath().map_shape(cli._operation_model.output_shape)
        shapes = ((path.to_jmespath(), remove_xml(shape.documentation)) for path, shape in shapes)
        print_completions(shapes)

    elif parsed.current_param in {'cli-input-json', 'cli-input-yaml'}:
        complete_files(parsed.current_fragment)

    elif [*commands, parsed.current_param] == ['cloudformation', 'deploy', 'template-file']:
        complete_files(parsed.current_fragment)

    elif match := re.match('file://|fileb://', parsed.current_fragment):
        complete_files(parsed.current_fragment[match.end():], prefix=match.group(0))

    elif (param := cli.arg_table.get(parsed.current_param)) and cli is not driver:

        if matches := list(complete_from_shape(param.argument_model, param)):
            print_completions(matches)

        elif commands[0] == 's3' and param.name == 'paths' and not param.argument_model:
            # complete local files
            if param._default != 's3://':
                doc = remove_xml(extract_doc(param.documentation))
                complete_files(parsed.current_fragment, doc=doc)

            if parsed.current_fragment.startswith('s3://'):
                # have some s3 path already
                bucket, slash, prefix = parsed.current_fragment.removeprefix('s3://').partition('/')
                if slash:
                    for page in driver.session.create_client('s3').get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
                        print_completions([(f's3://{bucket}/{x["Prefix"]}', '') for x in page.get('CommonPrefixes', ())], suffix='')
                        print_completions((f's3://{bucket}/{x["Key"]}', '') for x in page.get('Contents', ()))
                    return
            if 's3://'.startswith(parsed.current_fragment[:len('s3://')]):
                # grab some buckets
                for page in driver.session.create_client('s3').get_paginator('list_buckets').paginate():
                    print_completions((f's3://{x["Name"]}/', '') for x in page.get('Buckets', ()))

        # probably a file
        elif param.cli_name.endswith('-outfile') and isinstance(param.argument_model, botocore.model.StringShape):
            doc = remove_xml(extract_doc(param.documentation))
            complete_files(parsed.current_fragment, doc=doc)

        else:
            # uhhh the builtin server side compelter is not that great sometimes
            # e.g. trying to complete "aws ssm get-parameter --name ..." it fetches ssm doc names instead of ssm param names
            #  results = complete_from_completer(parsed, autocomplete.serverside.create_server_side_completer(None))
            results = None
            if results is None:
                # drop the verb
                info = commands[1].partition('-')[2]
                if results := grabber_service.Service(commands[0], driver.session).how_to_get(info + ' ' + parsed.current_param):
                    # use the best one
                    print('Running:', results[0])
                    for page in results[0].execute({}):
                        print_completions((x, '') for x in page)
            else:
                print_completions(results)

    if not parsed.current_param or parsed.parsed_params.get(parsed.current_param):
        print_completions(complete_from_subcommand_table(parsed, cli, driver))
        for required in (True, False):
            print_completions(complete_from_arg_table(parsed, cli, required))
            complete_from_arg_table_positional(parsed, cli, required)
            if cli is not driver:
                print_completions(complete_from_arg_table(parsed, driver, required))
