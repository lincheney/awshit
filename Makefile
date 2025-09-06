.ONESHELL:
.SHELLFLAGS = -eu -o pipefail -c
# .SILENT:

CLI_LEGACY_PLUGIN_PATH := $(shell python -c 'import os, botocore.session; print(os.path.expanduser(botocore.session.Session().full_config.get("plugins", {}).get("cli_legacy_plugin_path", "")))' )

all:

check-CLI_LEGACY_PLUGIN_PATH:
	@if [ -z '$(CLI_LEGACY_PLUGIN_PATH)' ]; then
		@echo ERROR: plugins.cli_legacy_plugin_path is not set
		@echo please run: aws configure set plugins.cli_legacy_plugin_path ...
		@echo
		exit 1
	fi

install-plugin-quick-aws: $(CLI_LEGACY_PLUGIN_PATH)/quick-aws
	aws configure set plugins.quick_aws_command_server quick-aws.command_server

$(CLI_LEGACY_PLUGIN_PATH)/quick-aws: ./quick-aws | check-CLI_LEGACY_PLUGIN_PATH
	ln -s "$$(realpath $<)" "$$(realpath $@)"
