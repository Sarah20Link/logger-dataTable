from __future__ import annotations

from io import StringIO
import time
from types import KeyTyping, RowTyping, RowValueTyping, SingleRowTyping
from typing import Any, Iterable, Literal

from exceptions import WrongArguments

# Im keeping this here for nostalgia
"""
Created on Tue Dec 21 00:36:25 2021

@author: sarah
"""

__all__ = ["DataTable", "pd_to_dataTable", "csv_to_dataTable"]


def floatable(txt: Any) -> bool:
    """Check whether a value can be converted to float.

    Args:
        txt: The value to test.

    Returns:
        True if ``txt`` can be cast to float, False otherwise.
        Booleans and empty strings always return False.

    Example:
        >>> floatable("3.14")
        True
        >>> floatable(True)
        False
        >>> floatable("")
        False
    """
    if isinstance(txt, bool) or txt == "":
        return False
    try:
        float(txt)
        return True
    except (ValueError, TypeError):
        return False


class DataTable:
    def __init__(
        self,
        column_keys: list,
        rows: dict[int | float, list[Any]] | None = None,
        directory: str = None,
        key_unit: Literal["s", "ns"] = "ns",
    ) -> None:
        """Create a DataTable with the given column headers and optional rows.

        Row keys are unix timestamps. Keys must be unique; inserting a duplicate
        key overwrites the existing row. For repeated indexes see DataTableMulti.

        Args:
            column_keys: Ordered list of column header names (excluding the index).
            rows: Optional pre-populated rows as ``{key: [values...]}``.
            directory: Optional file path used for CSV I/O.
            key_unit: Timestamp resolution – ``'s'`` for seconds,
                ``'ns'`` for nanoseconds.

        Raises:
            WrongArguments: If ``rows`` contains rows with inconsistent lengths.

        Example:
            >>> dt = DataTable(["price", "volume"])
            >>> dt.shape
            (0, 2)
        """
        self.header: list[str] = list(map(str, column_keys))
        self.rows: dict[int | float, list[Any]] = rows if rows is not None else {}
        self.dir: str = directory
        self.csv_updated = False
        self.__col_count: int = len(self.header)
        if self.rows:
            self.validate_row_count(self.rows)

        self.key_unit: Literal["s", "ns"] = key_unit
        self.__key_func = time.time if key_unit == "s" else time.time_ns()

    # .:: Maintenance ::.

    def copy(self) -> DataTable:
        """Return a shallow copy of this DataTable.

        Returns:
            A new DataTable with the same header and row data.

        Example:
            >>> dt2 = dt.copy()
            >>> dt2 == dt
            True
        """
        return self.__copy__()

    @staticmethod
    def validate_row_count(
        rows: dict[int | float, list[Any]], raise_error: bool = True
    ) -> bool:
        """Validate that all rows in a dict have the same number of columns.

        Args:
            rows: Row dict to validate.
            raise_error: If True, raise on mismatch instead of returning False.

        Returns:
            True if all rows have equal length, False otherwise.

        Raises:
            WrongArguments: If rows have inconsistent lengths and
                ``raise_error`` is True.
        """
        r = rows.values()
        _c = len(rows[0])
        bl = not any(len(row) != _c for row in r)
        if bl and raise_error:
            raise WrongArguments("Not all rows have the same number of columns.")
        return bl

    def validate_col_count(
        self,
        row: RowValueTyping | RowTyping | SingleRowTyping,
        raise_error: bool = True,
    ) -> bool:
        """Validate that a row has the same number of columns as this table.

        Args:
            row: The row to validate; accepted as a list, dict, or
                ``(key, values)`` tuple.
            raise_error: If True, raise on mismatch instead of returning False.

        Returns:
            True if the column count matches, False otherwise.

        Raises:
            WrongArguments: If the column count mismatches and
                ``raise_error`` is True.
        """
        if isinstance(row, RowValueTyping):
            ln = len(next(iter(row.values())))
        elif isinstance(row, RowTyping):
            ln = len(row)
        else:
            ln = len(row[1])
        bl = ln == self.__col_count
        if not bl and raise_error:
            raise WrongArguments(
                f"Given number of columns of {ln} does not match the number of column of DataTable of {self.__col_count}."
            )
        return bl

    def make_index_key(self) -> KeyTyping:
        """Generate a new unique timestamp key based on the table's ``key_unit``.

        Returns:
            Current time as an int (nanoseconds) or float (seconds).

        Example:
            >>> key = dt.make_index_key()
            >>> isinstance(key, (int, float))
            True
        """
        return self.__key_func()

    @property
    def shape(self) -> tuple[int, int]:
        """Return the (rows, columns) dimensions of the table.

        Returns:
            Tuple of ``(number_of_rows, number_of_columns)``.

        Example:
            >>> DataTable(["A", "B", "C"]).shape
            (0, 3)
        """
        return len(self.rows), len(self.header)

    @property
    def index(self) -> list[KeyTyping]:
        """Return a list of all row keys.

        Returns:
            Row keys in insertion order.

        Example:
            >>> dt.index
            [1700000000.0, 1700000001.0]
        """
        return list(self.rows.keys())

    @property
    def empty(self) -> bool:
        """Return True if the table contains no rows.

        Returns:
            True when the row count is zero.

        Example:
            >>> DataTable(["A"]).empty
            True
        """
        return len(self.rows) == 0

    def __copy__(self) -> DataTable:
        cop = DataTable(self.header_(), directory=self.get_directory())
        cop.dict_append(self.rows.copy())
        return cop

    def __eq__(self, other: DataTable) -> bool:
        if (
            type(self) == type(other)
            and self.header == other.header
            and self.rows == other.rows
        ):
            return True
        return False

    def __add__(self, other_row: RowValueTyping) -> DataTable:
        self.validate_col_count(other_row)
        self.list_append(other_row)

    def __iter__(self):
        for k in self.index:
            yield k

    def __repr__(self) -> str:
        headers = str(self.header)[1:-1].replace("'", "")
        msg = f"\t\t\t\t\t{self.header}\n"
        if len(self) == 0:
            return f"Empty dataTable.\nHeaders:\t\t\t\t\t{headers}\n"
        for i in self:
            msg = msg + f"{i}:\t\t{self[i]}\n"
        return msg

    def __str__(self) -> str:
        msg = f"\t\t\t\t\t{self.header}\n"
        if len(self) == 0:
            return f"Empty dataTable.\nHeaders:\t\t\t\t\t{str(self.header)[1:-1]}\n"
        for i in self:
            msg = msg + f"{i}:\t\t{self[i]}\n"

        return msg

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(
        self, key: KeyTyping | tuple[KeyTyping, ...] | slice
    ) -> RowValueTyping | DataTable:
        if isinstance(key, tuple):
            return self.get_rows_by_keys(*key)
        elif isinstance(key, slice):
            return self.get_slice(key.start, enddate=key.stop)
        return self.rows[key]

    def __setitem__(self, key: KeyTyping, val: RowValueTyping) -> None:
        if isinstance(val, list):
            self.validate_col_count(val)
            self.list_append(val, key=key)
        else:
            raise WrongArguments(f"{type(val)} Type was given while not permitted.")

    def __delitem__(self, key: KeyTyping) -> None:
        self.rows.__delitem__(key)

    def __contains__(self, key: KeyTyping) -> bool:
        return key in self.rows

    @classmethod
    def pd_to_dataTable(cls, df) -> DataTable:
        """Construct a DataTable from a pandas DataFrame.

        Args:
            df: A ``pandas.DataFrame`` to convert.

        Returns:
            A new DataTable with the same columns, index, and data.

        Example:
            >>> import pandas as pd
            >>> df = pd.DataFrame({"A": [1, 2]}, index=[1.0, 2.0])
            >>> dt = DataTable.pd_to_dataTable(df)
        """
        data = df.to_dict(orient="split")
        return cls(data["columns"], rows=dict(zip(data["index"], data["data"])))

    # .:: CSV handling ::.

    @classmethod
    def from_csv(
        cls, directory: str, delimiter: str = ",", ignore_invalid_rows: bool = True
    ) -> DataTable:
        """Load a DataTable from a CSV file.

        The CSV must have a header row. The first column is treated as the row
        key (must be numeric); the remaining columns map to the headers.

        Args:
            directory: Path to the CSV file.
            delimiter: Column delimiter character. Defaults to ``','``.
            ignore_invalid_rows: If True, skip rows whose key cannot be
                converted to float. If False, raise ``TypeError`` instead.

        Returns:
            A new DataTable populated from the CSV.

        Raises:
            TypeError: If a row key is non-numeric and
                ``ignore_invalid_rows`` is False.

        Example:
            >>> dt = DataTable.from_csv("data.csv")
        """
        rows = {}
        with open(directory, "r") as file:
            head = file.readline().split(delimiter)[1:]
            for line in file:
                if line != "":
                    row = line.split(sep=delimiter)
                    key = row.pop(0)
                    if floatable(key):
                        rows[float(key)] = row
                    elif not ignore_invalid_rows:
                        raise TypeError(f"Key of {key} should be of type int or float.")
        return cls(head, rows=rows)

    def to_csv(
        self, output: str | StringIO | None = None, file_mode: str = "w", sep: str = ","
    ) -> str | StringIO | None:
        """Serialize the DataTable to CSV format.

        Args:
            output: Destination for the CSV data.
                - ``None``: return the CSV as a string.
                - ``str``: write to the file at that path and return the path.
                - ``StringIO``: write into the buffer and return it seeked to 0.
            file_mode: File open mode when ``output`` is a path. Defaults to ``'w'``.
            sep: Column separator character. Defaults to ``','``.

        Returns:
            The CSV string, the file path, or the StringIO buffer depending on
            the type of ``output``.

        Example:
            >>> csv_str = dt.to_csv()
            >>> dt.to_csv("out.csv")
            'out.csv'
        """
        txt = sep.join(str(e) for e in self.header_()) + "\n"
        txt = f"date{sep}{txt}"
        for k, v in self.rows.items():
            txt = txt + f"{str(k)}{sep}{v}\n"

        if isinstance(output, StringIO):
            output.write(txt)
            output.seek(0)
            return output
        elif isinstance(output, str):
            with open(output, file_mode) as file:
                file.write(txt)
                return output
        else:
            return txt

    ######################
    ######################
    ######################
    ######################
    ######################
    ######################
    ######################
    ######################
    ######################
    ######################
    ######################

    def list_append(
        self, lst: RowValueTyping, key: KeyTyping | None = None
    ) -> KeyTyping:
        """Append a row given as a list, optionally at a specific key.

        Args:
            lst: Row values in column order.
            key: Row key (unix timestamp). Auto-generated if None.

        Returns:
            The key under which the row was stored.

        Raises:
            WrongArguments: If ``lst`` has the wrong number of columns or
                ``key`` cannot be converted to float.

        Example:
            >>> dt = DataTable(["A", "B"])
            >>> key = dt.list_append([1, 2])
            >>> dt[key]
            [1, 2]
        """
        if key is None:
            key = self.make_index_key()
        self.validate_col_count(lst)
        if floatable(key):
            self.rows[key] = lst.copy()
        else:
            raise WrongArguments(
                f'Given key of "{key}" could not be converted to float.'
            )
        return key

    def dict_append(self, data: RowTyping | SingleRowTyping) -> None:
        """Append one or more rows from a dict or a ``(key, values)`` tuple.

        Args:
            data: Either a ``{key: [values...]}`` dict or a
                ``(key, [values...])`` tuple.

        Raises:
            WrongArguments: If any row has the wrong number of columns.

        Example:
            >>> dt.dict_append({1700000000.0: [10, 20]})
            >>> dt.dict_append((1700000001.0, [30, 40]))
        """
        self.validate_col_count(data)
        if isinstance(data, dict):
            self.validate_row_count(data)
            self.rows.update(data)
        else:
            self.rows[data[0]] = data[1]

    def dict_extender(self, dic: dict[str, Any], key: KeyTyping | None = None) -> None:
        """Append a row from a column-name-keyed dict, filling missing columns with None.

        Args:
            dic: Mapping of column name to value,
                e.g. ``{"ticker": "AVAX-USDT", "side": "sell"}``.
            key: Row key. Auto-generated if None.

        Example:
            >>> dt = DataTable(["ticker", "side", "qty"])
            >>> dt.dict_extender({"ticker": "BTC-USDT", "side": "buy"})
            >>> dt[dt.index[-1]]
            ['BTC-USDT', 'buy', None]
        """
        dic_keys = dic.keys()
        self.list_append(
            [dic[i] if i in dic_keys else None for i in self.header], key=key
        )

    def drop_column(self, column: str) -> None:
        """Remove a column from the table in place.

        Args:
            column: Name of the column to drop.

        Example:
            >>> dt = DataTable(["A", "B", "C"])
            >>> dt.drop_column("B")
            >>> dt.header
            ['A', 'C']
        """
        ind = self.header_index(column)
        self.header.remove(column)
        for key in self:
            del self[key][ind]

    def rename_column(self, **column_name_map) -> None:
        """Rename one or more columns in place.

        Args:
            **column_name_map: Keyword arguments where each key is the current
                column name and the value is the new name.

        Example:
            >>> dt.rename_column(price="close", vol="volume")
        """
        for k, v in column_name_map.items():
            if k in self.header:
                self.header[self.header_index(k)] = v

    def add_column(
        self,
        other: DataTable,
        index: int = -1,
        prefix: str = "other_",
        add_prefix_if_not_exist: bool = False,
    ) -> None:
        """Insert columns from another DataTable into this one.

        For each column in ``other``, if the column name already exists in
        ``self`` (or ``add_prefix_if_not_exist`` is True), the column is
        inserted with ``prefix`` prepended to its name.

        Args:
            other: Source DataTable whose columns are inserted.
            index: Position at which to insert each column. Defaults to ``-1``
                (before the last column).
            prefix: Prefix applied to column names that conflict. Defaults to
                ``'other_'``.
            add_prefix_if_not_exist: If True, apply the prefix even when the
                column name doesn't conflict. Defaults to False.

        Example:
            >>> dt.add_column(other_dt, index=2, prefix="src_")
        """
        for col in other.header:
            if (col in self.header) or (
                col not in self.header and add_prefix_if_not_exist
            ):
                self.header.insert(index, f"{prefix}{col}")
            else:
                self.header.insert(index, col)
            for key in self:
                self.rows[key].insert(index, self[key][index] if key in other else None)

    def update_column(self, column: str, mapping: dict[KeyTyping, Any]) -> None:
        """Update specific cells in a column using a key-value mapping.

        Args:
            column: Name of the column to update.
            mapping: ``{row_key: new_value}`` pairs.

        Example:
            >>> dt.update_column("price", {1700000000.0: 99.5})
        """
        ind = self.header_index(column)
        for key, val in mapping:
            self[key][ind] = val

    def update_rows(self, keys: list[KeyTyping], mapping: dict[str, Any]) -> None:
        """Set the same column values across multiple rows.

        Args:
            keys: List of row keys to update.
            mapping: ``{column_name: new_value}`` pairs applied to every key.

        Example:
            >>> dt.update_rows([1700000000.0, 1700000001.0], {"status": "closed"})
        """
        ind_mp = {self.header_index(i): v for i, v in mapping.items()}
        for key in keys:
            for i, v in ind_mp.items():
                self[key][i] = v

    def row_to_dict(self, key: KeyTyping) -> dict:
        """Return a single row as a ``{column: value}`` dict.

        Args:
            key: The row key to convert.

        Returns:
            Ordered dict mapping each column name to its value.

        Example:
            >>> dt.row_to_dict(1700000000.0)
            {'price': 100.0, 'volume': 500}
        """
        return {c: self[key][i] for i, c in enumerate(self.header)}

    def update_cell(self, key: KeyTyping, column: Any, value: Any) -> None:
        """Set the value of a single cell.

        Args:
            key: Row key.
            column: Column name.
            value: New cell value.

        Example:
            >>> dt.update_cell(1700000000.0, "price", 101.5)
        """
        self.rows[key][self.header_index(column)] = value

    def get_rows_by_cell_value(self, *args: Any) -> DataTable:
        """Return all rows that contain any of the given values in any column.

        Args:
            *args: Values to search for across all columns.

        Returns:
            A new DataTable containing the matching rows.

        Example:
            >>> dt.get_rows_by_cell_value("buy", "sell")
        """
        d = DataTable(self.header_(), directory=self.dir)
        for i in self:
            for j in args:
                if j in self[i]:
                    d.list_append(self[i], key=i)
        return d

    def get_top_rows(self, numrows: int, top: bool = True) -> DataTable:
        """Return rows with the highest (or lowest) keys.

        Args:
            numrows: Number of rows to return.
            top: If True, return rows with the largest keys; if False, the
                smallest. Defaults to True.

        Returns:
            A new DataTable with at most ``numrows`` rows, or a full copy if
            the table has fewer rows than requested.

        Example:
            >>> dt.get_top_rows(3)          # 3 most recent rows
            >>> dt.get_top_rows(3, top=False)  # 3 oldest rows
        """
        if self.empty or numrows > len(self):
            return self.copy()

        k = self.index
        k.sort(reverse=top)
        temp_dict = {k[i]: self.rows[k[i]] for i in range(0, numrows)}
        return DataTable(self.header_(), directory=self.dir, rows=temp_dict)

    def get_rows_by_keys(self, *args: KeyTyping, raise_error: bool = True) -> DataTable:
        """Return a new DataTable containing only the specified row keys.

        Args:
            *args: Row keys to include.
            raise_error: If True, re-raise ``KeyError`` when a key is missing.

        Returns:
            A new DataTable with only the requested rows.

        Raises:
            KeyError: If any key is not found and ``raise_error`` is True.

        Example:
            >>> dt.get_rows_by_keys(1700000000.0, 1700000001.0)
        """
        try:
            return DataTable(
                self.header_(), directory=self.dir, rows={k: self[k] for k in args}
            )
        except KeyError:
            if raise_error:
                raise KeyError()

    def get_slice(
        self,
        strtdate: float,
        enddate: float = None,
        includeStrt: bool = True,
        includeEnd: bool = True,
    ) -> DataTable:
        """Return rows whose keys fall within a time range.

        Args:
            strtdate: Start of the range (unix timestamp).
            enddate: End of the range (unix timestamp). Defaults to now.
            includeStrt: Include the start boundary. Defaults to True.
            includeEnd: Include the end boundary. Defaults to True.

        Returns:
            A new DataTable with rows in the specified range.

        Raises:
            WrongArguments: If ``enddate`` is not greater than ``strtdate``.

        Example:
            >>> dt.get_slice(1700000000.0, 1700003600.0)
            >>> dt[1700000000.0:1700003600.0]  # equivalent slice syntax
        """
        if enddate is None:
            enddate = self.make_index_key()
        if enddate <= strtdate:
            raise WrongArguments("end date must be smaller than start date")
        d = DataTable(self.header_(), directory=self.dir, key_unit=self.key_unit)
        for i in self:
            s = (includeStrt and i >= strtdate) or (not includeStrt and i > strtdate)
            e = (includeEnd and i <= enddate) or (not includeEnd and i < enddate)
            if s and e:
                d.list_append(self[i], key=i)
        return d

    def get_cols(self, columns: list[str] | str) -> DataTable:
        """Extract one or more columns into a new DataTable.

        Args:
            columns: A column name or list of column names to extract.

        Returns:
            A new DataTable containing only the specified columns and all rows.

        Example:
            >>> dt.get_cols("price")
            >>> dt.get_cols(["price", "volume"])
        """
        if isinstance(columns, str):
            columns = [columns]
        d = DataTable(columns, directory=self.dir, key_unit=self.key_unit)
        for unix in self:
            d.list_append(
                [[self[unix][self.header_index(k)] for k in columns] for unix in self],
                key=unix,
            )
        return d

    def get_cell(self, key: KeyTyping, column: str) -> Any:
        """Return the value of a single cell.

        Args:
            key: Row key.
            column: Column name.

        Returns:
            The cell value.

        Example:
            >>> dt.get_cell(1700000000.0, "price")
            100.0
        """
        return self.rows[key][self.header_index(column)]

    def get_directory(self) -> str:
        """Return the file path associated with this table.

        Returns:
            The directory string set at construction, or None if not set.
        """
        return self.dir

    def get_row_count_by_col_value(self, column: Any, value: Any) -> int:
        """Count rows where a column equals a specific value.

        Args:
            column: Column name to inspect.
            value: Value to match against.

        Returns:
            Number of rows where ``column == value``.

        Example:
            >>> dt.get_row_count_by_col_value("side", "buy")
            42
        """
        index = self.header_index(column)
        return sum(self.rows[i][index] == value for i in self)

    def get_rows_by_col_value(self, mapping: dict[str, Iterable[Any]]) -> DataTable:
        """Return rows that match a set of allowed values per column.

        Args:
            mapping: ``{column_name: iterable_of_allowed_values}`` pairs.
                A row is included if its value for each column is in the
                corresponding allowed values.

        Returns:
            A new DataTable with the matching rows.

        Example:
            >>> dt.get_rows_by_col_value({"side": ["buy"], "status": ["open", "partial"]})
        """
        rss = set(self.index)
        dt = DataTable(self.header_(), directory=self.dir, key_unit=self.key_unit)
        for col, vals in mapping.items():
            rss -= set(dt.rows.keys())
            col_ind = self.header_index(col)
            for key in rss:
                if self[key][col_ind] in vals:
                    dt.list_append(self[key], key=key)
        return dt

    def header_(self) -> list:
        """Return a copy of the column header list.

        Returns:
            A new list containing the column names.

        Example:
            >>> dt.header_()
            ['price', 'volume']
        """
        return self.header.copy()

    def header_index(self, column_key: str) -> int:
        """Return the positional index of a column by name.

        Args:
            column_key: Column name to look up.

        Returns:
            Zero-based index of the column.

        Raises:
            ValueError: If ``column_key`` is not in the header.

        Example:
            >>> dt.header_index("volume")
            1
        """
        return self.header.index(column_key)

    # .:: Math ::.

    def abs_col(self, column_key: str) -> DataTable:
        """Return a new single-column DataTable with absolute values of a column.

        Args:
            column_key: Name of the column to process.

        Returns:
            A new DataTable with column ``'results'`` containing absolute values.

        Example:
            >>> dt.abs_col("pnl")
        """
        index = self.header_index(column_key)
        return DataTable(
            ["results"], rows={key: [abs(self[key][index])] for key in self}
        )

    def sum_of_col(self, column_key: str, raise_error: bool = True) -> float:
        """Return the sum of all numeric values in a column.

        Args:
            column_key: Name of the column to sum.
            raise_error: If True, re-raise any conversion error encountered.
                If False, skip non-numeric values silently. Defaults to True.

        Returns:
            Sum of the column values as a float.

        Example:
            >>> dt.sum_of_col("volume")
            15000.0
        """
        summ = 0
        index = self.header_index(column_key)
        for i in self:
            try:
                summ += float(self.rows[i][index])
            except:
                if raise_error:
                    raise
        return summ

    def average(self, column_key: str) -> float:
        """Return the arithmetic mean of a column.

        Args:
            column_key: Name of the column to average.

        Returns:
            Mean value as a float.

        Example:
            >>> dt.average("price")
            102.5
        """
        return self.sum_of_col(column_key) / len(self)

    def multiply_col(self, other: DataTable | int | float) -> DataTable:
        """Multiply a single-column table element-wise by another or a scalar.

        Args:
            other: A single-column DataTable or a numeric scalar.

        Returns:
            A new DataTable with column ``'results'`` containing the products.

        Raises:
            WrongArguments: If either operand has more than one column.

        Example:
            >>> dt.multiply_col(2)
            >>> dt.multiply_col(other_dt)
        """
        if (self.shape[1] != 1) or (isinstance(other, DataTable) and other.shape != 1):
            raise WrongArguments(
                "Multiplication is only allowed between two one column dataframe, or one column dataframe and a number"
            )
        if isinstance(other, DataTable):
            return DataTable(
                ["results"], rows={key: [self[key][0] * other[key][0]] for key in self}
            )
        return DataTable(
            ["results"], rows={key: [self[key][0] * other] for key in self}
        )

    def divide_col(self, other: DataTable | int | float) -> DataTable:
        """Divide a single-column table element-wise by another or a scalar (self / other).

        Args:
            other: A single-column DataTable or a numeric scalar.

        Returns:
            A new DataTable with column ``'results'`` containing the quotients.

        Raises:
            WrongArguments: If either operand has more than one column.

        Example:
            >>> dt.divide_col(100)
            >>> dt.divide_col(other_dt)
        """
        if (self.shape[1] != 1) or (isinstance(other, DataTable) and other.shape != 1):
            raise WrongArguments(
                "Division is only allowed between two one column dataframe, or one column dataframe and a number"
            )
        if isinstance(other, DataTable):
            return DataTable(
                ["results"], rows={key: [self[key][0] / other[key][0]] for key in self}
            )
        return DataTable(
            ["results"], rows={key: [self[key][0] / other] for key in self}
        )

    def add_col(self, other: DataTable | int | float) -> DataTable:
        """Add a single-column table element-wise to another or a scalar.

        Args:
            other: A single-column DataTable or a numeric scalar.

        Returns:
            A new DataTable with column ``'results'`` containing the sums.

        Raises:
            WrongArguments: If either operand has more than one column.

        Example:
            >>> dt.add_col(10)
            >>> dt.add_col(other_dt)
        """
        if (self.shape[1] != 1) or (isinstance(other, DataTable) and other.shape != 1):
            raise WrongArguments(
                "Addition is only allowed between two one column dataframe, or one column dataframe and a number"
            )
        if isinstance(other, DataTable):
            return DataTable(
                ["results"], rows={key: [self[key][0] + other[key][0]] for key in self}
            )
        return DataTable(
            ["results"], rows={key: [self[key][0] + other] for key in self}
        )

    def subtract_col(self, other: DataTable | int | float) -> DataTable:
        """Subtract another or a scalar from a single-column table element-wise (self - other).

        Args:
            other: A single-column DataTable or a numeric scalar.

        Returns:
            A new DataTable with column ``'results'`` containing the differences.

        Raises:
            WrongArguments: If either operand has more than one column.

        Example:
            >>> dt.subtract_col(5)
            >>> dt.subtract_col(other_dt)
        """
        if (self.shape[1] != 1) or (isinstance(other, DataTable) and other.shape != 1):
            raise WrongArguments(
                "Subtraction is only allowed between two one column dataframe, or one column dataframe and a number"
            )
        if isinstance(other, DataTable):
            return DataTable(
                ["results"], rows={key: [self[key][0] - other[key][0]] for key in self}
            )
        return DataTable(
            ["results"], rows={key: [self[key][0] - other] for key in self}
        )

    def mod_col(self, other: DataTable | int | float) -> DataTable:
        """Apply modulo element-wise on a single-column table (self % other).

        Args:
            other: A single-column DataTable or a numeric scalar.

        Returns:
            A new DataTable with column ``'results'`` containing the remainders.

        Raises:
            WrongArguments: If either operand has more than one column.

        Example:
            >>> dt.mod_col(10)
        """
        if (self.shape[1] != 1) or (isinstance(other, DataTable) and other.shape != 1):
            raise WrongArguments(
                "Modulo is only allowed between two one column dataframe, or one column dataframe and a number"
            )
        if isinstance(other, DataTable):
            return DataTable(
                ["results"], rows={key: [self[key][0] % other[key][0]] for key in self}
            )
        return DataTable(
            ["results"], rows={key: [self[key][0] % other] for key in self}
        )

    def period_return(self, column: str, descending: bool = False) -> DataTable:
        """Compute the period-over-period percentage return for a column.

        Each value is computed as ``(current - previous) / previous``.
        The first row (oldest key) is excluded from the result.

        Args:
            column: Column name to compute returns for.
            descending: If True, sort keys in descending order before computing.
                Defaults to False (ascending / chronological).

        Returns:
            A new DataTable with column ``'results'`` containing the returns.

        Example:
            >>> dt.period_return("close")
        """
        colkey_index = self.header_index(column)
        keys = sorted(self.index, reverse=descending)
        res = {}
        for i in range(1, len(keys)):
            res[keys[i]] = [
                (self[keys[i]][colkey_index] - self[keys[i - 1]][colkey_index])
                / self[keys[i - 1]][colkey_index]
            ]

        return DataTable(["results"], rows=res)

    def pct(self, x1_column: str, x2_column: str) -> DataTable:
        """Compute ``(x2 - x1) / x1`` element-wise between two columns.

        Args:
            x1_column: Name of the base column (denominator).
            x2_column: Name of the comparison column (numerator after subtraction).

        Returns:
            A new single-column DataTable with the percentage change values.

        Example:
            >>> dt.pct("open", "close")
        """
        x1_col = self.get_cols(x1_column)
        dt = self.get_cols(x2_column).subtract_col(x1_col).divide_col(x1_col)
        return dt

    def table_duration(self) -> float:
        """Return the time span between the first and last row keys.

        Returns:
            Absolute difference between the maximum and minimum row keys.

        Example:
            >>> dt.table_duration()
            3600.0
        """
        r = sorted(self.index)
        return abs(max(r) - min(r))

    def to_pandas(self) -> "pandas.DataFrame":
        """Convert this DataTable to a pandas DataFrame.

        Returns:
            A ``pandas.DataFrame`` with the row keys as the index and the
            column headers preserved.

        Example:
            >>> df = dt.to_pandas()
            >>> type(df)
            <class 'pandas.core.frame.DataFrame'>
        """
        import pandas

        return pandas.DataFrame.from_dict(
            self.rows, orient="index", columns=self.header
        )
