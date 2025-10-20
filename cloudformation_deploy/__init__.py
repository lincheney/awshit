import os
import re
import sys
import time
import shlex
import subprocess
from functools import cache, wraps

import botocore.client
import botocore.waiter
import botocore.exceptions

RESET = '\x1b[0m'
BOLD = '\x1b[1m'
RED = '\x1b[91m'
GREEN = '\x1b[92m'
YELLOW = '\x1b[93m'
BLUE = '\x1b[94m'
PURPLE = '\x1b[95m'

@cache
def isatty(file):
    return os.isatty(file.fileno())

@cache
def get_diff_filter():
    return subprocess.run(['git', 'config', '--get', 'interactive.diffFilter'], text=True, capture_output=True).stdout or 'cat'

def patch(obj, name):
    def wrapper(replacement, obj=obj, name=name):
        original_fn = getattr(obj, name)
        @wraps(original_fn)
        def wrapped(*args, original_fn=original_fn, **kwargs):
            return replacement(original_fn, *args, **kwargs)
        setattr(obj, name, wrapped)
        return wrapped
    return wrapper

def _print(*args, colour='', file=sys.stderr, end='\n', **kwargs):
    if colour and isatty(file):
        print(colour, end='', file=file)
        print(*args, end='', file=file, **kwargs)
        print(RESET, end=end, file=file)
    else:
        print(*args, end=end, file=file, **kwargs)

def awscli_initialize(event_hooks):
    # patches

    @patch(botocore.client.BaseClient, '_make_api_call')
    def _make_api_call(super, self, operation_name, api_params):
        if self._service_model.service_name == 'cloudformation' and operation_name == 'ExecuteChangeSet':
            before_execute_changeset(self, api_params)
        return super(self, operation_name, api_params)

    @patch(botocore.client.BaseClient, 'get_waiter')
    def get_waiter(super, self, name):
        waiter = super(self, name)
        if self._service_model.service_name == 'cloudformation' and name in {'stack_create_complete', 'stack_update_complete', 'stack_import_complete', 'stack_delete_complete', 'stack_rollback_complete'}:
            waiter._operation_method = EventTailer(self, waiter._operation_method).invoke
        return waiter

def before_execute_changeset(client, api_params):
    user_agent = client._user_agent_creator.to_string()
    if user_agent.endswith('cloudformation.deploy'):
        changes = client.describe_change_set(ChangeSetName=api_params['ChangeSetName'], IncludePropertyValues=True)
        stack_name = changes['StackName']
        _print('Changes for:', stack_name, colour=BOLD)

        for change in changes['Changes']:

            change = change['ResourceChange']
            action = change['Action']
            if action == 'Modify':
                action = {'Conditional': 'May replace', 'True': 'Replace'}.get(change.get('Replacement'), action).strip()
            colour = {'Add': GREEN, 'Modify': GREEN, 'Remove': RED}.get(action, YELLOW)

            _print(action, change['LogicalResourceId'], 'caused by:' if change['Details'] else '', colour=colour, sep='\t')

            seen = set()
            details = sorted(change['Details'] or (), key=lambda detail: (detail['Target']['Path'], 'CausingEntity' not in detail))
            for detail in details:
                path = detail['Target']['Path']
                if path in seen:
                    continue
                seen.add(path)

                cause = path
                if 'CausingEntity' in detail:
                    cause = detail['CausingEntity'] + ' -> ' + cause
                _print('\t-', cause + ':', colour='\x1b[1m')
                before = shlex.quote(detail['Target'].get('BeforeValue', ''))
                after = shlex.quote(detail['Target'].get('AfterValue', ''))
                script_sh = rf'''diff -u3 <(printf '%s\n' {before}) <(printf '%s\n' {after}) | ({get_diff_filter()}) | tail -n+3'''
                subprocess.run(['bash', '-c', script_sh])

        _print()

        os.environ.setdefault('AWS_EXECUTE_CHANGESET', '10')
        if os.environ['AWS_EXECUTE_CHANGESET'] in ('no', '0'):
            raise Exception(f'AWS_EXECUTE_CHANGESET={os.environ["AWS_EXECUTE_CHANGESET"]} given')
        elif os.environ['AWS_EXECUTE_CHANGESET'].lower() == 'ask':
            while True:
                response = input('Proceed (y/n): ')
                if response.lower() in ('y', 'yes'):
                    _print()
                    break
                if response.lower() in ('n', 'no'):
                    raise Exception('Abort')
        elif os.environ['AWS_EXECUTE_CHANGESET'].isnumeric():
            _print('Pausing for', os.environ['AWS_EXECUTE_CHANGESET'], 'seconds')
            time.sleep(int(os.environ['AWS_EXECUTE_CHANGESET']))

class EventTailer:
    def __init__(self, client, operation_method):
        self.client = client
        self.operation_method = operation_method
        self.started = False
        self.last_event = None
        self.stack_name = None
        self.stack_id = None
        self.resource_id_width = 0

    def get_events_starting_from(self, event_id=None):
        try:
            events = self.client.describe_stack_events(StackName=self.stack_id or self.stack_name)['StackEvents']
        except botocore.exceptions.ClientError as e:
            if re.fullmatch(r'Stack .* does not exist', e.response['Error']['Message']):
                return ()
            raise

        for i, e in enumerate(events):
            self.stack_id = e['StackId']
            if not event_id and e['PhysicalResourceId'] == e['StackId']:
                if e.get('ResourceStatusReason') == 'User Initiated':
                    return events[:i+1]
                break
            elif e['EventId'] == event_id:
                return events[:i]
        return ()

    def invoke(self, *args, StackName, **kwargs):
        self.stack_name = StackName
        if not self.started:
            _print('Events for: ', self.stack_name, colour=BOLD)
            self.started = True

        if events := self.get_events_starting_from(self.last_event):
            self.last_event = events[0]['EventId']

            colours = {
                'ROLLBACK_FAILED': RED,
                'ROLLBACK_IN_PROGRESS': RED,
                'FAILED': RED,
                'ROLLBACK': PURPLE,
                'IN_PROGRESS': YELLOW,
                # 'SKIPPED': BLUE,
                'COMPLETE': GREEN,
            }

            self.resource_id_width = max(self.resource_id_width, max(len(e['LogicalResourceId']) for e in events))
            for e in reversed(events):
                colour = next((v for k, v in colours.items() if k in e['ResourceStatus']), '')
                if e['PhysicalResourceId'] == self.stack_id:
                    colour += BOLD
                _print(e['Timestamp'].rpartition('T')[2].partition('+')[0], end='')
                _print(
                    '',
                    e['ResourceStatus'].ljust(27),
                    e['LogicalResourceId'].ljust(self.resource_id_width),
                    e.get('ResourceStatusReason', ''),
                    colour=colour,
                    sep='  '
                )

        return self.operation_method(*args, **kwargs)
