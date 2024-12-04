for i in {1..10000}; do
	/Users/maggielou/bb/buildbuddy/bazel-bin/cli/cmd/bb/bb_/bb remote --script="exit 1"
	exit_code=$?
	if [[ $exit_code -ne 1 ]]; then
	    echo "Error: Command exited with code $exit_code instead of 1"
	    exit 1
	else
	    echo "Command exited with the expected code 1."
	fi
done
