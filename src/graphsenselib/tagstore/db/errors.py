from typing import Optional


class TagAlreadyExistsException(Exception):
    """Tag with the same key already exists"""

    def __init__(self, existing_id: Optional[str] = None):
        super().__init__()
        # report id (context uuid) of the tag already stored, if known
        self.existing_id = existing_id
