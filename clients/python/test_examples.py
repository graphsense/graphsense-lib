# ruff: noqa: T201
import pypandoc
import json
import os
import glob
import sys
from io import StringIO
from contextlib import redirect_stdout

pattern = "*Api"
api_key = os.environ.get("GS_API_KEY") or os.environ.get("API_KEY") or "YOUR_API_KEY"
replace = os.environ.get("REPLACE_API_URL")

# Some generated snippets read API_KEY directly from the environment.
# Keep it non-empty so the client sends a real header value instead of None.
if not os.environ.get("API_KEY"):
    os.environ["API_KEY"] = api_key

exclude_regexes = [
    "report_tag"  # This api has side effects, so skip it in CI and testing.
]
issues = 0

for fil in glob.glob("./docs/" + pattern + ".md"):
    print(fil)
    data = pypandoc.convert_file(fil, "json")

    for block in json.loads(data)["blocks"]:
        if block["t"] != "CodeBlock":
            continue
        code = block["c"][1].replace("YOUR_API_KEY", api_key)

        if any(regex in block["c"][1] for regex in exclude_regexes):
            print(
                f"Skipping execution of snippet in {fil} due to matching exclude regex"
            )
            continue

        if replace:
            url = replace.split(">")
            code = code.replace(url[0], url[1])
        f = StringIO()
        try:
            with redirect_stdout(f):
                exec(code)
            output = f.getvalue()
            if "Exception" in output:
                print(output)
                issues += 1
            else:
                print("Executed successfully")

        except Exception as e:
            # Some generated examples instantiate required models with empty payloads.
            # Keep running remaining samples and report the failure instead of aborting.
            print(code.replace(api_key, "XXX"))
            print(f"Exception while executing snippet: {e}\n")
            issues += 1

if issues > 0:
    print(f"Encountered {issues} snippet issue(s).")
    sys.exit(1)
