import re
import sys


def parse_struct_types(lines):
    types = []
    cur_struct_type = None
    cur_comment = ""
    last_comment = ""
    line_number = -1

    def consume_comment():
        nonlocal last_comment
        if not last_comment:
            raise ValueError(
                f"Line {line_number} is missing markdown documentation (must start with ///)"
            )
        c = last_comment
        last_comment = ""
        return c

    for i, line in enumerate(lines):
        line_number = i + 1
        line = line.strip()
        line = re.sub(r"\s+", " ", line)

        # Accumulate comments.
        if line.startswith("///"):
            line = re.sub(r"^///\s?", "", line) + "\n"
            cur_comment += line
            continue
        elif cur_comment != "":
            last_comment = cur_comment
            cur_comment = ""

        if cur_struct_type is not None:
            if line == "}":
                # Reached end of struct.
                cur_struct_type = None
                continue

            m = re.search(r"^[A-Z]", line)
            if m:
                # Parse struct field.
                field_name, go_type, tag = line.split(" ")

                m = re.search(r'yaml:"(.*?)"', tag)
                if not m:
                    raise ValueError(
                        f'Missing `yaml:"<field-name>"` annotation on {cur_struct_type["name"]}.{field_name}'
                    )

                field_type = {
                    "name": re.sub(r"[\[\]\*]", "", go_type),
                    "is_list": go_type.startswith("[]"),
                }

                cur_struct_type["fields"].append(
                    {
                        "name": m.group(1),
                        "type": field_type,
                        "description": consume_comment(),
                    }
                )
        else:
            m = re.match(r"type\s+(.*)\s+struct {", line)
            if m:
                cur_struct_type = {
                    "name": m.group(1),
                    "description": consume_comment(),
                    "fields": [],
                }
                types.append(cur_struct_type)
                continue

    return types


def render_markdown(structs):
    lines = []
    for struct in structs:
        lines.append(f'## `{struct["name"]}` {{#{slugify(struct["name"])}}}\n')
        lines.append("\n")
        lines.append(f'{struct["description"]}\n')
        lines.append("\n")

        lines.append("| Field | Type | Description |\n")
        lines.append("| ------|------|-------------|\n")
        for field in struct["fields"]:
            field_type = field["type"]
            name = field["name"]
            type_name = field_type["name"]
            linkified_type_name = (
                f"`{type_name}`"
                if is_primitive(type_name)
                else f"[`{type_name}`](#{slugify(type_name)})"
            )
            type_list_qualifier = " list" if field_type["is_list"] else ""
            # Escape double newlines (\n\n) as <br><br> and single newlines as regular spaces,
            # since \n starts a new table row.
            description = field["description"]
            description = description.replace("\n\n", "<br /><br />")
            description = description.replace("\n", " ")
            lines.append(
                f"| `{name}` | {linkified_type_name}{type_list_qualifier} | {description} |\n"
            )

        lines.append("\n")

    return lines


GENERATED_FILE_NOTICE = """<!--

GENERATED FILE; DO NOT EDIT.

Edit workflows-config-header.md to edit the header contents
or update workflow/config/generate_docs.py to change how the
schema documentation is displayed.

Re-generate by running:

python3 workflow/config/generate_docs.py

-->
"""

METADATA_SECTION = """---
id: workflows-config
title: Workflows configuration
sidebar_label: Workflows configuration
---
"""


def main():
    with open("enterprise/server/workflow/config/config.go") as f:
        lines = f.readlines()

    structs = parse_struct_types(lines)

    md_lines = render_markdown(structs)

    with open("docs/workflows-config-header.md", "r") as f:
        header_lines = f.readlines()

    # Place generated file notice after the metadata section at the top.
    notice_index = -1
    for (i, line) in enumerate(header_lines):
        if i > 0 and line.strip() == "---":
            notice_index = i + 1
            break

    with open("docs/workflows-config.md", "w") as f:
        f.writelines(METADATA_SECTION)
        f.writelines("\n")
        f.writelines(GENERATED_FILE_NOTICE)
        f.writelines("\n")
        f.writelines(header_lines)
        f.writelines("\n")
        f.writelines(md_lines)


def slugify(name):
    out = name.replace("_", "-")
    out = re.sub(r"([A-Z])", r"-\1", name)
    if out.startswith("-"):
        out = out[1:]
    out = re.sub(r"-+", "-", out)
    out = out.lower()
    # Handle special cases (sigh)
    out = out.replace("build-buddy", "buildbuddy")

    return out


def is_primitive(name):
    return name[0].lower() == name[0]


if __name__ == "__main__":
    try:
        main()
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)