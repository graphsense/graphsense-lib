def get_exception_digest(ex):
    return f"{type(ex).__name__}: {str(ex)}"
