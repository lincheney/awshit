import awscli.formatter
import awscli.utils
import json

class JSONDumper:
    @staticmethod
    def dump(value, stream):
        json.dump(value, stream, ensure_ascii=False, default=awscli.utils.json_encoder)
        stream.write('\n')

class StreamedJSONFormatter(awscli.formatter.StreamedYAMLFormatter):
    '''
    Print newline-delimited JSON for each response page
    '''

    def __init__(self, args):
        super().__init__(args, yaml_dumper=JSONDumper)

def awscli_initialize(event_hooks):
    event_hooks.register('building-command-table.main', hook)

def hook(command_table, session, command_object, **kwargs):
    choices = command_object._get_cli_data()['options']['output']['choices']
    if 'json-stream' not in choices:
        choices.append('json-stream')
        awscli.formatter.CLI_OUTPUT_FORMATS['json-stream'] = StreamedJSONFormatter
