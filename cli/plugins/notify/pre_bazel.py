import os
import sys
import time

if __name__ == "__main__":
    with open(sys.argv[1], 'r') as args_file:
        arg_lines = args_file.readlines()

    enabled_via_env_var = os.environ.get('BB_NOTIFY', '0') in ('1', 'true')
    if '--notify\n' not in arg_lines and not enabled_via_env_var:
        exit(0)
    # Consume --notify arg if it exists
    arg_lines = [line for line in arg_lines if line != '--notify\n']
    with open(sys.argv[1], 'w') as args_file:
        args_file.writelines(arg_lines)

    # Create the file "${PLUGIN_TEMPDIR}/notify" to inform the post_bazel hook
    # that notifications are enabled for this invocation.
    with open(os.path.join(os.environ['PLUGIN_TEMPDIR'], 'notify'), 'w') as f:
        pass
