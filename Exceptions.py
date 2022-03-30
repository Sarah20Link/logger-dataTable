

class wrongArguments(Exception):
    def __init__(self, err=""):
        message = f"wrong type or number of arguments passed into the function, {err}"
        super().__init__(message)


class valueNotFound(Exception):
    def __init__(self, err=""):
        message = f"input value not fount in the sequence of arguments, {err}"
        super().__init__(message)

