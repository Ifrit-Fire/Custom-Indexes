class APILimitReachedError(Exception):
    """Raised when an API rate limit is reached."""
    pass


class NoResultsFoundError(Exception):
    """Raised when a result was expected, but the API returned nothing."""
    pass
