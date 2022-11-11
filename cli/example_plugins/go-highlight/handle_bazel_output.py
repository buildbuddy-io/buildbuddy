import re
import sys

if __name__ == "__main__":
    for line in sys.stdin:
        m = re.search(r"^(.*?\.go:\d+:\d+:)(.*)", line)
        if m:
            print("\x1b[33m" + m.group(1) + "\x1b[0m" + m.group(2))
        else:
            print(line, end="")
