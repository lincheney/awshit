import os

class ExceptionHandler:
    @staticmethod
    def handle_exception(exception, stdout, stderr):
        raise exception

def awscli_initialize(event_hooks):
    event_hooks.register('building-command-table.main', hook)

def hook(command_table, session, command_object, **kwargs):
    if os.environ.get('AWSHIT_EXCEPTION') == '1':
        command_object._error_handler = ExceptionHandler
