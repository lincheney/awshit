import os
import traceback
import awscli.constants

class ExceptionHandler:
    @staticmethod
    def handle_exception(exception, stdout, stderr):
        traceback.print_exception(exception)
        return awscli.constants.GENERAL_ERROR_RC

def awscli_initialize(event_hooks):
    event_hooks.register('building-command-table.main', hook)

def hook(command_table, session, command_object, **kwargs):
    if os.environ.get('AWSHIT_EXCEPTION') == '1':
        command_object._error_handler = ExceptionHandler
