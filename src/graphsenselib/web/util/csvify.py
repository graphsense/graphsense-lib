from csv import DictWriter

from graphsenselib.errors import BadUserInputException
from werkzeug.datastructures import Headers

from graphsenselib.web.models import Values


def create_download_header(filename):
    headers = Headers()
    headers.add("Content-Disposition", "attachment", filename=filename)
    return headers


class writer:
    def write(self, str):
        self.str = str

    def get(self):
        return self.str


def to_csv(query_function):
    flat_dict = {}
    page_state = None
    wr = writer()
    csv = None
    while True:
        (page_state, rows) = query_function(page_state)
        if rows is None:
            raise BadUserInputException("nothing found")

        def flatten(item, name=""):
            if isinstance(item, Values):
                flat_dict[name + "value"] = item.value
                for rate in item.fiat_values:
                    flat_dict[name + rate.code] = rate.value
                return
            if "to_dict" in dir(item):
                item = item.to_dict()
            if isinstance(item, dict):
                for sub_item in item:
                    flatten(item[sub_item], name + sub_item + "_")
            else:
                flat_dict[name[:-1]] = item

        for row in rows:
            flatten(row)
            if not csv:
                fieldnames = sorted(flat_dict.keys())
                csv = DictWriter(wr, fieldnames)
                csv.writeheader()
                yield wr.get()

            csv.writerow(flat_dict)
            yield wr.get()
            flat_dict = {}

        if not page_state:
            break
