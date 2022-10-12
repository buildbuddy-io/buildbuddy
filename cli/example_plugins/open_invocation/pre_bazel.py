import os
import sys
import uuid

def main():
    args_file_path = sys.argv[1]

    with open(args_file_path, 'r') as f:
        args = f.read().split('\n')

    bes_results_url = get_flag_value(args, 'bes_results_url')

    # TODO: More robust parsing (handle --open false, --noopen, --open=1 etc.)
    if '--open' not in args or not bes_results_url:
        return

    args.remove('--open')

    # Add --invocation_id flag if not already set.
    invocation_id = get_flag_value(args, 'invocation_id')
    if not invocation_id:
        invocation_id = str(uuid.uuid4())
        args.append('--invocation_id=' + invocation_id)

    # Write a temp file with the invocation URL so that the post-bazel hook
    # knows which URL to open.
    url_file_path = os.path.join(os.environ['PLUGIN_TEMPDIR'], 'invocation_url.txt')
    with open(url_file_path, 'w') as f:
        f.write(bes_results_url + invocation_id)

    # Write the new args.
    with open(args_file_path, 'w') as f:
        f.write('\n'.join(args))


# TODO: Provide a library function for this?
def get_flag_value(flags, name):
    for flag in flags:
        if flag.startswith('--' + name + '='):
            return '='.join(flag.split('=')[1:])
    return None


if __name__ == "__main__":
    main()
