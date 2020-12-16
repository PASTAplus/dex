import werkzeug.exceptions


class RedirectToIndex(werkzeug.exceptions.HTTPException):
    code = 302
    description = "RedirectToIndex"


class DexError(werkzeug.exceptions.HTTPException):
    pass


class EMLError(DexError):
    pass
