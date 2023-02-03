import sys
import json
import os

def main():
    with open(sys.argv[1], 'r') as args_file:
        arg_lines = args_file.readlines()

    positional_args = [arg.strip("\n") for arg in arg_lines if not arg.startswith("--")]
    if len(positional_args) != 2:
        return
    if positional_args[0] != 'run':
        return
    try:
        with open(os.path.join(os.path.dirname(__file__), "config.json"), 'r') as config_file:
            cfg = json.load(config_file)
    except FileNotFoundError:
        return

    additional_exec_args = cfg.get(positional_args[1], [])
    if len(additional_exec_args) == 0:
        return

    with open(sys.argv[2], 'r') as exec_args_file:
        exec_arg_lines = exec_args_file.readlines()

    with open(sys.argv[2], 'w') as exec_args_file:
        exec_args_file.writelines(additional_exec_args + exec_arg_lines)

    with open(sys.argv[2], 'r') as exec_args_file:
        exec_arg_lines = exec_args_file.readlines()

if __name__ == "__main__":
    main()
