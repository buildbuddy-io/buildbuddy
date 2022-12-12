import os
import sys
import uuid

def main():
    args_file_path = sys.argv[1]

    with open(args_file_path, 'r') as f:
        args = f.read().split('\n')


    # TODO: More robust parsing (handle --open false, --noopen, --open=1 etc.)
    args, url_suffix = consume_open_arg(args)
    if url_suffix is None:
        return

    bes_results_url = get_flag_value(args, 'bes_results_url')
    if bes_results_url:
        write_invocation_url(args, bes_results_url, url_suffix)
    else:
        sys.stderr.write("\x1b[33mWarning: --bes_results_url is not configured; --open flag will be ignored\x1b[m\n")

    # Write the new args.
    with open(args_file_path, 'w') as f:
        f.write('\n'.join(args))

def consume_open_arg(args):
    new_args = []
    url_suffix = None
    for arg in args:
        if arg == '--open':
            url_suffix = ''
            continue
        if arg.startswith('--open='):
            url_suffix = arg[len('--open='):]
            continue
        new_args.append(arg)
    return new_args, url_suffix

def write_invocation_url(args, bes_results_url, url_suffix):
    # Add --invocation_id flag if not already set.
    invocation_id = get_flag_value(args, 'invocation_id')
    if not invocation_id:
        invocation_id = str(uuid.uuid4())
        args.append('--invocation_id=' + invocation_id)

    # Write a temp file with the invocation URL so that the post-bazel hook
    # knows which URL to open.
    url_file_path = os.path.join(os.environ['PLUGIN_TEMPDIR'], 'invocation_url.txt')
    with open(url_file_path, 'w') as f:
        f.write(bes_results_url + invocation_id + url_suffix)


# TODO: Provide a library function for this?
def get_flag_value(flags, name):
    """Returns the last occurrence of the given flag name."""
    value = None
    for flag in flags:
        if flag.startswith('--' + name + '='):
            value = '='.join(flag.split('=')[1:])
    return value


if __name__ == "__main__":
    main()
