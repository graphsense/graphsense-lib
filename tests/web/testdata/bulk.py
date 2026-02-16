from tests.web.testdata.blocks import block, block2

error_bodies = [{"x": "x"}, {}]
block_path = "/{currency}/bulk.{form}/get_block?num_pages=1"
headers = {"Accept": "application/json", "Authorization": "x"}
