#!/usr/bin/env python3

import sys
import re
from functools import partial
import itertools
import html
import botocore.model
import awscli.autocomplete.main as autocomplete

def awscli_initialize(event_hooks):
    event_hooks.register('building-command-table.main', inject_commands)

def inject_commands(command_table, session, command_object, **kwargs):
    command_table['.tab-completion'] = partial(start_server, command_object)

class CommandLine:
    def __init__(self, inner):
        self.inner = inner
    def __getitem__(self, key):
        return ' '.join(self.inner)[key]
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

def complete_from_subcommand_table(table, driver):
    for name, cmd in table.items():
        doc = ''
        if hasattr(cmd, 'service_model'):
            doc = cmd.service_model.service_id
        elif table is driver.subcommand_table and hasattr(cmd, 'NAME'):
            doc = cmd.NAME
        elif hasattr(cmd, 'create_help_command') and (help := cmd.create_help_command()):
            if table is driver.subcommand_table:
                doc = help.description.split('.')[0].strip()
            elif hasattr(help, 'description'):
                doc = help.description
            elif hasattr(help, 'obj') and hasattr(help.obj, 'documentation'):
                doc = help.obj.documentation
            else:
                doc = help.doc.getvalue().decode('utf8')
        yield name, remove_xml(extract_doc(doc))

def complete_from_arg_table(table):
    for v in table.values():
        doc = remove_xml(extract_doc(v.documentation))
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

def start_server(driver, argv, opts=None):
    completer = autocomplete.create_autocompleter(driver=driver)
    # we've already presplit the command
    argv = CommandLine(argv)
    parsed = completer._parser.parse(argv)

    commands = parsed.lineage[1:]
    if commands and parsed.current_command and parsed.current_fragment is not None:
        commands.append(parsed.current_command)
    #  print(f'''DEBUG(dater) \t{vars(parsed) = }''', file=sys.__stderr__)

    global_flags = dict(complete_from_arg_table(driver.arg_table))

    cli = driver
    for c in commands:
        cli = cli.subcommand_table[c]

    #  results = completer.autocomplete(argv)
    #  for r in results:
        #  print(f'''DEBUG(gnat)  \t{r.name,r.help_text = }''', file=sys.__stderr__)
    #  return

    if parsed.current_param:
        param = cli.arg_table[parsed.current_param]
        for name, doc in complete_from_shape(param.argument_model, param):
            print(f'''DEBUG(awol)  \t{name, doc = }''', file=sys.__stderr__)
    else:
        for name, doc in complete_from_subcommand_table(cli.subcommand_table, driver):
            print(f'''DEBUG(invoke)\t{name, doc = }''', file=sys.__stderr__)
        for name, doc in complete_from_arg_table(cli.arg_table):
            print(f'''DEBUG(invoke)\t{name, doc = }''', file=sys.__stderr__)
