from __future__ import annotations

from io import StringIO
from types import KeyTyping, RowTyping, RowValueTyping, SingleRowTyping
from typing import Any, Tuple, Union, Literal, Iterable

from exceptions import ValueNotFound, WrongArguments

"""
Created on Tue Dec 21 00:36:25 2021

@author: sarah
"""

__all__ = ["DataTable", "pd_to_dataTable", "csv_to_dataTable"]


def floatable(txt: Any) -> bool:
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
        misc: Any = None,
        key_unit: Literal['s', 'ns'] = 'ns'
    ) -> None:
        """
        column_keys : list of column titles in order excluding date
        keys that are dates are  time in unix seconds
        keys in the table must be unique and new rows appended will replace the existing keys, for repeated indexes
            please check out the solution in dataTable_multi class
        """
        # self.keys = column_keys.copy()
        self.header: list[str] = list(map(str, column_keys))
        self.rows: dict[int | float, list[Any]] = rows if rows is not None else {}
        self.dir: str = directory
        self.csv_updated = False
        self.__col_count: int = len(self.header)
        if self.rows:
            self.validate_row_count(self.rows)

        self.key_unit: Literal['s', 'ns'] = key_unit
        self.__key_func = time.time if key_unit == 's' else time.time_ns()

    # .:: Maintenance ::.
    def copy(self) -> DataTable:
        return self.__copy__()

    @staticmethod
    def validate_row_count(
        rows: dict[int | float, list[Any]], raise_error: bool = True
    ) -> bool:
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
        return self.__key_func()

    @property
    def shape(self) -> tuple[int, int]:
        return len(self.rows), len(self.header)



    @property
    def index(self) -> list[KeyTyping]:
        return list(self.rows.keys())

    @property
    def empty(self) -> bool:
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
        for k in tuple(self.rows.keys()):
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

    def __getitem__(self, key: KeyTyping | tuple[KeyTyping, ...] | slice) -> RowValueTyping | DataTable:
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

    @classmethod
    def pd_to_dataTable(cls, df) -> DataTable:
        data = df.to_dict(orient="split")
        return cls(data["columns"], rows=dict(zip(data["index"], data["data"])))

    # .:: CSV handling ::.
    @classmethod
    def from_csv(
        cls, directory: str, delimiter: str = ",", ignore_invalid_rows: bool = True
    ) -> DataTable:
        """
        the csv file must have a header row and has to have a character or a free space for the index header
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
        """
        dic = {key: [row1, row2, ...]}
        """
        self.validate_col_count(data)
        if isinstance(data, dict):
            self.validate_row_count(data)
            self.rows.update(data)
        else:
            self.rows[data[0]] = data[1]

    def dict_extender(self, dic: dict[str, Any], key: KeyTyping | None = None) -> None:
        """
        {"ticker":"AVAX-USDT", "side":"sell", ...}
        """
        dic_keys = dic.keys()
        self.list_append(
            [dic[i] if i in dic_keys else None for i in self.header], key=key
        )


    def drop_column(self, column: str) -> None:
        ind = self.header_index(column)
        self.header.remove(column)
        for key in self:
            del self[key][ind]


    def update_column(self, column: str, mapping: dict[KeyTyping, Any]) -> None:
        ind = self.header_index(column)
        for key, val in mapping:
            self[key][ind] = val

    def update_rows(self, keys: list[KeyTyping], mapping: dict[str, Any]) -> None:
        ind_mp = {self.header_index(i): v for i, v in mapping.items()}
        for key in keys:
            for i, v in ind_mp.items():
                self[key][i] = v


    def row_to_dict(self, key: KeyTyping) -> dict:
        return {c: self[key][i] for i, c in enumerate(self.header)}


    def update_cell(self, key: KeyTyping, column: Any, value: Any) -> None:
        self.rows[key][self.header_index(column)] = value


    def get_rows_by_cell_value(self, *args) -> DataTable:
        """
        Returns
        -------
        d : DataTable
        """
        d = DataTable(self.header_(), directory=self.dir)
        for i in self:
            for j in args:
                if j in self[i]:
                    d.list_append(self[i], key=i)
        return d

    def get_top_rows(self, numrows: int, top: bool = True) -> DataTable:
        """
        numrows : number of rows to be returned.
        top : if true it returns the highest and if false returns the lowest. The default is True.
        """
        if self.empty or numrows > len(self):
            return self.copy()

        k = self.index
        k.sort(reverse=top)
        temp_dict = {k[i]: self.rows[k[i]] for i in range(0, numrows)}
        return DataTable(self.header_(), directory=self.dir, rows=temp_dict)


    def get_rows_by_keys(self, *args, raise_error: bool = True) -> DataTable:
        try:
            return DataTable(self.header_(), directory=self.dir, rows={k: self[k] for k in args})
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
        """
        strtdate :  in unix seconds.
        enddate :  in unix seconds, end date not included.
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
        """
        Fish out a column or multiple columns according to the input
        colkey and returns it in a anew dataTable
        """
        if isinstance(columns, str):
            columns = [columns]
        d = DataTable(columns, directory=self.dir, key_unit=self.key_unit)
        for unix in self:
            d.list_append([[self[unix][self.header_index(k)] for k in columns] for unix in self], key=unix)
        return d

    def get_cell(self, key: KeyTyping, column: str) -> Any:
        return self.rows[key][self.header_index(column)]

    def get_directory(self) -> str:
        return self.dir

    def get_row_count_by_col_value(self, column: Any, value: Any) -> int:
        """
        return number of crows that have the same colvalue at the specified colkey
        """
        index = self.header_index(column)
        return sum(self.rows[i][index] == value for i in self)

    def get_rows_by_col_value(
        self, mapping: dict[str, Iterable[Any]]
    ) -> DataTable:
        rss = set(self.rows.keys())
        dt = DataTable(self.header_(), directory=self.dir, key_unit=self.key_unit)
        for col, vals in mapping.items():
            rss -= set(dt.rows.keys())
            col_ind = self.header_index(col)
            for key in rss:
                if self[key][col_ind] == vals:
                    dt.list_append(self[key][col_ind], key=key)
        return dt

    def header_(self) -> list:
        return self.header.copy()

    def header_index(self, column_key: str) -> int:
        return self.header.index(column_key)


    def merger(self, table: DataTable) -> None:
        """
        adds  cols in table that are not in self to self, non-existing row values should be filled with none
        """
        # TODO
        for head in table.header_():
            if head not in self.header_():
                self.header.append(head)
                for row in self:
                    row_lst = self[row]
                    if row not in table.row_keys():
                        row_lst.append(None)
                    else:
                        row_lst.append(table.get_cell(row, head))
                    self[row] = row_lst
        table_width = self.shape[1]
        for head in table.header_():
            for row in table.row_keys():
                if row not in self.row_keys():
                    lst = []
                    for i in range(table_width - 1):
                        lst.append(None)
                    lst.append(table.get_cell(row, head))
                    self.list_append(row, lst)
        temp = []
        for i in table.header_():
            if i not in self.header_():
                temp.append(i)
        if len(temp) > 0:
            fish = table.fish(temp)
            self.header.extend(temp)
            for row in fish:
                if row in self.rows:
                    self.rows[row].extend(table[row])
                else:
                    r = table[row]
                    for i in range(0, (len(self.header) - len(temp))):
                        r.insert(0, None)
                    self.list_append(row, r)

    # .:: Math ::.
    def abs_col(self, column_key: str) -> DataTable:
        """
        perform get absolute value of a column
        """
        index = self.header_index(column_key)
        return DataTable(["results"], rows={key: [abs(self[key][index])] for key in self})

    def sum_of_col(self, column_key: str, raise_error: bool = True) -> float:
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
        return self.sum_of_col(column_key) / len(self)

    def multiply_col(self, other: DataTable | int | float) -> DataTable:
        """
        perform Multiplication between two cols
        self and other is a datatable with one column
        """
        if (self.shape[1] != 1) or (isinstance(other, DataTable) and other.shape != 1):
            raise WrongArguments(
                "Multiplication is only allowed between two one column dataframe, or one column dataframe and a number"
            )
        if isinstance(other, DataTable):
            return DataTable(["results"], rows={key: [self[key][0] * other[key][0]] for key in self})
        return DataTable(["results"], rows={key: [self[key][0] * other] for key in self})

    def divide_col(self, other: DataTable | int | float) -> DataTable:
        """
        perform division between two cols, self/other
        self and other is a datatable with one column
        """
        if (self.shape[1] != 1) or (isinstance(other, DataTable) and other.shape != 1):
            raise WrongArguments(
                "Division is only allowed between two one column dataframe, or one column dataframe and a number"
            )
        if isinstance(other, DataTable):
            return DataTable(["results"], rows={key: [self[key][0] / other[key][0]] for key in self})
        return DataTable(["results"], rows={key: [self[key][0] / other] for key in self})

    def add_col(self, other: DataTable | int | float) -> DataTable:
        """
        perform division between two cols, self/other
        self and other is a datatable with one column
        """
        if (self.shape[1] != 1) or (isinstance(other, DataTable) and other.shape != 1):
            raise WrongArguments(
                "Addition is only allowed between two one column dataframe, or one column dataframe and a number"
            )
        if isinstance(other, DataTable):
            return DataTable(["results"], rows={key: [self[key][0] + other[key][0]] for key in self})
        return DataTable(["results"], rows={key: [self[key][0] + other] for key in self})

    def subtract_col(self, other: DataTable | int | float) -> DataTable:
        """
        perform division between two cols, self/other
        self and other is a datatable with one column
        """
        if (self.shape[1] != 1) or (isinstance(other, DataTable) and other.shape != 1):
            raise WrongArguments(
                "Subtraction is only allowed between two one column dataframe, or one column dataframe and a number"
            )
        if isinstance(other, DataTable):
            return DataTable(["results"], rows={key: [self[key][0] - other[key][0]] for key in self})
        return DataTable(["results"], rows={key: [self[key][0] - other] for key in self})


    def mod_col(self, other: DataTable | int | float) -> DataTable:
        """
        perform division between two cols, self/other
        self and other is a datatable with one column
        """
        if (self.shape[1] != 1) or (isinstance(other, DataTable) and other.shape != 1):
            raise WrongArguments(
                "Modulo is only allowed between two one column dataframe, or one column dataframe and a number"
            )
        if isinstance(other, DataTable):
            return DataTable(["results"], rows={key: [self[key][0] % other[key][0]] for key in self})
        return DataTable(["results"], rows={key: [self[key][0] % other] for key in self})


    def period_return(
        self, column: str, descending: bool = False
    ) -> DataTable:
        colkey_index=self.header_index(column)
        keys = sorted(self.rows.keys(), reverse=descending)
        res = {}
        for i in range(1, len(keys)):
            res[keys[i]]= [(
                self[keys[i]][colkey_index] - self[keys[i - 1]][colkey_index]
            ) / self[keys[i - 1]][colkey_index]]

        return DataTable(["results"], rows=res)


    def pct(self, x1_column: str, x2_column: str) -> DataTable:
        x1_col = self.get_cols(x1_column)
        dt = self.get_cols(x2_column).subtract_col(x1_col).divide_col(x1_col)
        return dt


    def table_duration(self) -> float:
        """
        returns the duration passed between the first entry and the last one
        """
        r = sorted(self.rows.keys())
        return abs(max(r) - min(r))

    def to_pandas(self) -> "pandas.DataFrame":
        import pandas
        return pandas.DataFrame.from_dict(
            self.rows, orient="index", columns=self.header
        )





if __name__ == "__main__":
    # numOfRows = 1000000
    # d= ["ticker", "side", "type", "margin", "algo", "conditional", "oid", "fee",
    #     "p", "q", "rpt", "r-r", "borrowSize", "loanApplyId", "marginCall"]
    # # append
    # startTime = time.perf_counter()
    # f = dataTable(d)
    # for i in range(0,numOfRows):
    #     #f.dict_append(({np.random.randint(100): [np.random.randint(100), 3,np.random.randint(1000),
    #     np.random.randint(100), np.random.randint(1000),np.random.randint(100),np.random.randint(100),
    #     np.random.randint(1000),np.random.randint(1000),np.random.randint(100), np.random.randint(1000)]}))
    #     f.dict_append({i:[(np.random.randint(100)) for a in d]})
    # # df=f.to_pandas()
    # print('Elapsed time: {:6.3f} seconds for {:d} rows'.format(time.perf_counter() - startTime, numOfRows))
    # print(f.shape)

    # # dict
    # startTime = time.perf_counter()
    # row_list = []
    # for i in range (0,5):
    #     row_list.append(dict( (a,np.random.randint(100)) for a in d))
    # for i in range( 1,numOfRows-4):
    #     dict1 = dict( (a,np.random.randint(100)) for a in d)
    #     row_list.append(dict1)

    # df4 = pandas.DataFrame(row_list, columns=['A','B','C','D','E'])
    # print('Elapsed time: {:6.3f} seconds for {:d} rows'.format(time.perf_counter() - startTime, numOfRows))
    # print(df4.shape)

    # -------------------------------------------------------------------------------

    # print(f)

    import time

    st = time.time()
    f = DataTable(["A", "B", "C", "D"])
    f.dict_append(({st: [5, 3, 100, 100]}))
    st = time.time()
    f[st] = [3, 56, 100, 75]
    st = time.time()
    f[st] = [43, 345, 120, 100]
    st = time.time()
    f[st] = [367, 589798, 100, 150]
    st = time.time()
    f[st] = [0, 3, 90, 110]
    st = time.time()
    f.list_append(st, [333, 555, 180, 90])

    # print(f)
    # import Algo
    # print(f.sum_col("99988"))
    # print("________________________________________________ \n")
    # print("##########")
    # print("test filo")
    # rint(f)
    # for i in f:
    #     print(i)
    # print(f[f.row_keys()[3]])
    st = time.time()
    f.list_append(st, [333, 555, 444])
    print(f)

    f.update_cell(st, "B", [34, "XX"])
    print(f)
    t = f.get_rows_by_col_value(["B"], [555]).top_row()[1]
    print(type(t))
    print(t)
    # print(f)
    # print("##########")
    # q = f.fish([1,12])
    # g = (f.get_rows_by_col_value(['1', '12'], [[345, 555], 3]))
    # s = g.__str__()
    # print(g)
    # print("######ewer####")
    # r = f.get_top_rows()

    # enddate = None, includeStrt = True, includeEnd = False, numrows=None, top = True):

    # print(f.get_rows())
    # print(list(f.get_top_rows(1).header())[0])
    # print("##########")
    # print(f.header_index(99988))
    # print(f.sum_(12))

    # print("________________________________________________ \n")
    # st = time.time()
    # f[st] = [367,589798,434]
