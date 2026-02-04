import pypandoc
import json
import os
import glob
from io import StringIO
from contextlib import redirect_stdout

pattern = "*Api"
api_key = os.environ.get("GS_API_KEY")
replace = os.environ.get("REPLACE_API_URL")

for fil in glob.glob("./docs/" + pattern + ".md"):
    print(fil)
    data = pypandoc.convert_file(fil, "json")

    for block in json.loads(data)["blocks"]:
        if block["t"] != "CodeBlock":
            continue
        code = block["c"][1].replace("YOUR_API_KEY", api_key)
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

        except Exception as e:
            print(code.replace(api_key, "XXX"))
            raise e
