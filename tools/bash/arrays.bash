# Given a separator like "--" as the first arg,
# splits the remaining args into arrays BEFORE and AFTER.
# example: split_args -- a b c -- d e f
# results in BEFORE=(a b c) and AFTER=(d e f).
function split_args() {
  BEFORE_SEPARATOR=()
  AFTER_SEPARATOR=()
  local separator="$1"
  shift
  local saw_separator=0
  for arg in "$@"; do
    if ((saw_separator)); then
      AFTER_SEPARATOR+=("$arg")
    elif [[ "$arg" == "$separator" ]]; then
      saw_separator=1
    else
      BEFORE_SEPARATOR+=("$arg")
    fi
  done
}
