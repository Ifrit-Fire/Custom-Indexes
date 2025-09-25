import pandas as pd

from src.consts import COL_MC, CRITICAL_COLUMNS
from src.data.source import ProviderSource
from src.logger import timber


class Reconciler:
    """
    Tracks and merges partial data from multiple providers into a single-row DataFrame.

    Used to incrementally build a complete data row, based on required columns. These are considered key columns that
    must not be invalid.  Fields are filled only if missing, and the reconciler tracks readiness once all required data
    is present.
    """

    def __init__(self):
        self._col_ready = self._col_ready = {col: False for col in CRITICAL_COLUMNS}
        self._data = pd.DataFrame()
        self._source = None
        self._is_ready = False

    @property
    def data(self):
        """
        Returns:
            The merged single-row DataFrame with current data.
        """
        return self._data

    @property
    def source(self):
        """
        Returns:
            The first provider that returned valid data.
        """
        return self._source

    @property
    def is_ready(self) -> bool:
        """
        Returns:
            True if all required fields are valid and the data is considered complete.
        """
        return self._is_ready

    def add(self, data: pd.DataFrame, source: ProviderSource):
        """
        Merges new data from a provider into the current result.

        If this is the first non-empty DataFrame, it becomes the base. For all subsequent providers, only
        missing or invalid fields are merged in. Readiness is updated based on whether all required fields
        are now valid.

        Args:
            data: Single-row DataFrame returned by a provider.
            source: The provider that returned the data.
        """
        log = timber.plant()
        if data.empty:
            log.debug("NoResultsFoundError", provider=source, response="switch providers")
            return

        if self._data.empty:
            self._data = data
            self._source = source
        else:
            for col in self._col_ready.keys():
                if not self._col_ready[col] and col in data.columns:
                    self._data.loc[0, col] = data.loc[0, col]

        for col in self._col_ready.keys():
            if col in self._data.columns:
                self._col_ready[col] = self._data[col].notna().all()
            else:
                self._col_ready[col] = True

        if COL_MC in self._data.columns and self._data.loc[0, COL_MC] == 0:
            self._col_ready[COL_MC] = False

        self._is_ready = all(self._col_ready.values())
