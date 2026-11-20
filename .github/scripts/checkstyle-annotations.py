#!/usr/bin/env python3
"""Turn Checkstyle XML reports into GitHub Actions workflow-command annotations.

Usage: checkstyle-annotations.py <report.xml>...

Replaces the third-party checkstyle-github-action (#238): workflow commands need
no token and no code from outside the repository. Each finding becomes an
::error, ::warning or ::notice line, with the file path made relative to the
workspace so GitHub can place it on the diff. Missing or unreadable reports are
skipped, so the step never fails the job; the Gradle task already decides that.
"""

import os
import sys
import xml.etree.ElementTree as ET

LEVELS = {"error": "error", "warning": "warning", "info": "notice"}


def escape_data(value):
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def escape_property(value):
    return escape_data(value).replace(":", "%3A").replace(",", "%2C")


def main(paths):
    workspace = os.environ.get("GITHUB_WORKSPACE", os.getcwd())
    total = 0
    for path in paths:
        try:
            root = ET.parse(path).getroot()
        except (OSError, ET.ParseError) as e:
            print(f"Skipping {path}: {e}")
            continue
        for file_el in root.iter("file"):
            name = os.path.relpath(file_el.get("name", ""), workspace)
            for err in file_el.iter("error"):
                level = LEVELS.get(err.get("severity", "error"), "warning")
                source = err.get("source", "").rsplit(".", 1)[-1].removesuffix("Check")
                props = [f"file={escape_property(name)}"]
                for key in ("line", "column"):
                    if err.get(key):
                        props.append(f"{'col' if key == 'column' else key}={err.get(key)}")
                if source:
                    props.append(f"title={escape_property('Checkstyle ' + source)}")
                print(f"::{level} {','.join(props)}::{escape_data(err.get('message', ''))}")
                total += 1
    print(f"{total} Checkstyle finding(s) in {len(paths)} report(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
