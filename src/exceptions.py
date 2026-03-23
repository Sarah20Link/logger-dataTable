class WrongArguments(Exception):
    def __init__(self, err=""):
        message = f"wrong type or number of arguments passed into the function, {err}"
        super().__init__(message)
