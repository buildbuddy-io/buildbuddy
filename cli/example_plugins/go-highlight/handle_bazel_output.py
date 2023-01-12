import re
import sys

if __name__ == "__main__":
    for line in sys.stdin:
        m = re.search(r"^(.*?\.go:\d+:\d+:)(.*)", line)
        if m:
            print("\x1b[33m" + m.group(1) + "\x1b[0m" + m.group(2))
            continue

        # TypeScript formats line numbers like "path.tsx(line,col)". In addition
        # to highlighting, reformat as "path:line:col:" since VSCode allows
        # Ctrl+Clicking to jump to that exact location in the code when it's
        # formatted that way.
        m = re.search(r"^(.*?\.tsx?)\((\d+),(\d+)\)(:)(.*)", line)
        if m:
            print("\x1b[33m%s:%s:%s%s\x1b[m%s" % m.groups())
            continue

        print(line, end="")
