from __future__ import annotations
from typing import (Any, Tuple, Union)
import Exceptions
from Utilities import floatable

"""
Created on Tue Dec 21 00:36:25 2021

@author: sarah
"""

__all__ = ["dataTable", "dataTable_multi", "pd_to_dataTable", "csv_to_dataTable"]


class dataTable:
    header: list[str]
    rows: dict[float, list]

    def __all__(self):
        return ["__copy__", "__eq__"]

    def __copy__(self) -> dataTable:
        cop = dataTable(self.header_(), self.get_directory())
        cop.dict_append(self.rows_())
        return cop

    def __eq__(self, other: dataTable) -> bool:
        if type(self) == type(other):
            if self.header == other.header:
                if self.rows == other.rows:
                    return True
        return False

    def __init__(self, colKeys: list, directory: str = None, misc: Any = None) -> None:
        """
        colKeys : list of column titles in order excluding date
        keys that are dates are instances of the class time in unix seconds
        keys in the table must be unique and new rows appended will replace the existing keys, for repeated indexes
            please check out the solution in dataTable_multi class
        """
        # self.keys = colKeys.copy()
        self.header: list = list(map(str, colKeys))
        self.rows: dict = {}
        self.dir: str = directory
        self.miscellaneous = misc
        self.csv_updated = False
        self.indexx = -1

    def __iter__(self):
        return (i for i in self.rows)

    def __repr__(self):
        headers = str(self.header)[1:-1].replace("'", "")
        msg = f"\t\t\t\t\t{self.header}\n"
        if len(self) == 0:
            return f"Empty dataTable.\nHeaders:\t\t\t\t\t{headers}\n"
        for i in self:
            msg = msg + f"{i}:\t\t{self[i]}\n"

        return msg

    def __str__(self):
        msg = f"\t\t\t\t\t{self.header}\n"
        if len(self) == 0:
            return f"Empty dataTable.\nHeaders:\t\t\t\t\t{str(self.header)[1:-1]}\n"
        for i in self:
            msg = msg + f"{i}:\t\t{self[i]}\n"

        return msg

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, key):
        if key in self.header:
            return self.get_cols(list(key))
        elif key in self.rows:
            return self.rows[key].copy()
        raise Exceptions.valueNotFound()

    def __setitem__(self, key, val):
        if isinstance(val, list):
            self.list_append(key, val)
        else:
            temp_var = str(type(val))[7:-1]
            raise Exceptions.wrongArguments(f"{temp_var} Type was given while not permitted")

    def __delitem__(self, key: float) -> None:
        # del(self[i])
        del [self.rows[key]]

    def average(self, colKey: str) -> float:
        s = self.sum_(colKey)
        l = len(self)
        return s / l

    def copy(self) -> dataTable:
        return self.__copy__()

    def CSV_maker(self, directory: str = None) -> None:
        if directory is None:
            dir_ = self.dir
        else:
            dir_ = directory
        with open(dir_, "w") as csv_file:
            txt = ",".join(str(e) for e in self.header_()) + "\n"
            csv_file.write(f"date,{txt}")

    def CSV_appender(self, directory: str = None) -> None:
        dir_ = directory
        if directory is None:
            dir_ = self.dir
        with open(dir_, "a") as file_appender:
            r = self.row_keys()
            r.sort()
            for i in r:
                txt = str(i) + "," + ",".join(str(e) for e in self.rows[i]) + "\n"
                file_appender.write(txt)

    def CSV_DT_updater(self, directory: str = None, drop_duplicates: bool = True) -> None:
        if len(self) > 0:
            if directory is not None:
                csv = csv_to_dataTable(directory)
            else:
                csv = csv_to_dataTable(self.dir)
            csv = csv.row_keys()
            csv.sort()
            keys = self.row_keys()
            keys.sort()
            last_data = keys[0]
            include_start = True
            if len(csv) > 0:
                last_data = float(csv[-1])
                include_start = False
            dt = self.get_rows(last_data, includeStrt=include_start, includeEnd=True)
            if drop_duplicates is True:
                for r in dt.row_keys():
                    if r in csv:
                        del dt[r]
            if directory is not None:
                dt.CSV_appender(directory=directory)
            else:
                dt.CSV_appender()
            if self.csv_updated is True:
                for k in keys:
                    if k <= last_data:
                        del self[k]
            self.csv_updated = True

    def dict_append(self, dic: dict) -> None:
        """
        dic = {key: [row1, row2, ...]}
        """
        for date in dic:
            date = float(date)
            if len(dic[date]) > len(self.header):
                raise Exceptions.wrongArguments(
                    "number of keys in the input dict can not be more than the the number of keys in the dataTable")
            else:
                if len(self.header) > len(dic[date]):
                    temp_var = len(self.header) - len(dic[date])
                    for i in range(0, temp_var):
                        dic[date].append(None)
                self.rows[date] = dic[date]

    def dict_extender(self, key: float, dic: dict) -> None:
        """
            {"ticker":"AVAX-USDT", "side":"sell", ...}
        """
        dic_keys = list(dic.keys())
        lst = []
        for i in self.header:
            if i in dic_keys:
                lst.append(dic[i])
            else:
                lst.append(None)
        self.list_append(key, lst)

    def FILO_date(self, data, max_dates):
        """
              data for lst: (time, [etrg,etg,etg])
              data for dic: {1234: [333, 555, 444]}
        """
        if isinstance(data, (dict, tuple)):
            if isinstance(data, dict):
                self.dict_append(data)
            else:
                self.list_append(data[0], data[1])
            if len(self) > max_dates:
                d = list(self.get_top_rows(len(self) - max_dates, top=False).keys())
                for i in d:
                    del (self[i])
        else:
            temp_var = str(type(data))[7:-1]
            raise Exceptions.wrongArguments(f"{temp_var} Type was given while dict and tuple is permitted")

    def find_by_cell(self, celVal: list[Any]) -> dataTable:
        """
        Returns
        -------
        d : DataTable
        """
        d = dataTable(self.header_(), directory=self.dir)
        for i in self:
            for j in celVal:
                if j in self[i]:
                    d.list_append(i, self[i])
        return d

    def get_top_rows(self, numrows: int, top: bool = True) -> dataTable:
        """
        numrows : number of rows to be returned.
        top : if true it returns the highest and if false returns the lowest. The default is True.
        """
        k = list(self.rows.keys())
        if len(k) == 0:
            return self.copy()
        if numrows > len(k):
            return self.copy()
        k.sort(reverse=top)
        temp_dict = {}
        for i in range(0, numrows):
            temp_dict[k[i]] = self.rows[k[i]]
        d = dataTable(self.header_(), directory=self.dir)
        d.dict_append(temp_dict)
        return d

    def get_rows(self, strtdate: float, enddate: float = None, includeStrt: bool = True, includeEnd: bool = False,
                 numrows: int = None, top: bool = True) -> dataTable:
        """
        strtdate :  in unix seconds.
        enddate :  in unix seconds, end date not included.
        """
        if enddate is None:
            enddate = time.time()
        if enddate <= strtdate:
            raise Exceptions.wrongArguments("end date must be smaller than start date")
        temp_dict = {}
        for i in list(self.rows.keys()):
            if (includeStrt is False) and (includeEnd is False):
                if (i > strtdate) and (i < enddate):
                    temp_dict[i] = self.rows[i]
            if (includeStrt is True) and (includeEnd is False):
                if (i >= strtdate) and (i < enddate):
                    temp_dict[i] = self.rows[i]
            if (includeStrt is False) and (includeEnd is True):
                if (i > strtdate) and (i <= enddate):
                    temp_dict[i] = self.rows[i]
            else:
                if (i >= strtdate) and (i <= enddate):
                    temp_dict[i] = self.rows[i]
        d = dataTable(self.header_(), directory=self.dir)
        d.dict_append(temp_dict)
        if numrows is not None:
            d = d.get_top_rows(numrows, top=top)
        return d

    def get_slice(self, startKey: float, endKey: float = None, numRows: int = None, less: bool = True) -> dataTable:
        if (endKey is None) and (numRows is None):
            raise Exceptions.wrongArguments("Either end key or num rows must be given")
        if isinstance(less, bool) is False:
            raise Exceptions.wrongArguments("'less' argument has to be type bool")
        dates = self.row_keys()
        dates.sort()
        startIndex = dates.index(startKey)
        if endKey is not None:
            endIndex = dates.index(endKey)
            if startIndex < endIndex:
                keys = dates[startIndex:endIndex + 1]
            else:
                keys = dates[endIndex:startIndex + 1]
        else:
            if less is True:
                keys = dates[startIndex + 1 - numRows:startIndex + 1]
            else:
                keys = dates[startIndex:startIndex + numRows]
        dt = dataTable(self.header_(), directory=self.dir)
        for k in keys:
            dt.list_append(k, self[k])
        return dt

    def get_cols(self, colKey: str) -> dataTable:
        """
            Fish out a column or multiple colums according to the input colkey and returns it in a anew dataTable
        """
        if isinstance(colKey, list) is False:
            temp_var = str(type(colKey))[7:-1]
            raise Exceptions.wrongArguments(f"{temp_var} Type was given while not permitted")
        dic: dict[float, list] = {i: [] for i in self}
        for k in colKey:
            for i in self:
                dic[i].append(self.rows[i][self.header.index(k)])
        d = dataTable(colKey)
        d.dict_append(dic)
        return d

    def get_row_cell_value(self, rowkey: float, colkey: str) -> Any:
        index = self.header.index(colkey)
        return self.rows[rowkey][index]

    def get_directory(self) -> str:
        return self.dir

    def get_num_rows_by_col_value(self, colkey: Any, colval: Any) -> int:
        """
            return number of crows that have the same colvalue at the specified colkey
        """
        index = self.header.index(colkey)
        count = 0
        for i in self:
            if self.rows[i][index] == colval:
                count += 1
        return count

    def get_rows_by_col_value(self, colkey: list, colval: list, numrows: int = 0, top: bool = True) -> dataTable:
        # TODO maybe swap the 'for' loops "j in self" and "i in range(len(colkey))". check the performace dif in a test
        """
            return rows that have the same colvalue at the specified colkey
            for multiple colvalues in a sepcific colkey put all the colvals insude a lsit
            when multiple values used, it does not search on top of each parameter, the return includes all results

            ex:
                dT.get_rows_by_col_value(['A', 'B'], [[valA1, valA2], valB])
        """
        if (not isinstance(colkey, list)) or (not isinstance(colval, list)):
            temp_var = str(type(colkey))[7:-1]
            temp_var1 = str(type(colval))[7:-1]
            raise Exceptions.wrongArguments(f"{temp_var}, {temp_var1} Type was given while lists are permitted")
        if len(colkey) != len(colval):
            raise Exceptions.wrongArguments("colkey and colval should be the same length")
        d = dataTable(self.header_(), directory=self.dir)
        for i in range(len(colkey)):
            index = self.header_index(colkey[i])
            for j in self:
                if (isinstance(colval[i], list)) and (self[j][index] in colval[i]):
                    d.list_append(j, self.rows_()[j])
                else:
                    if self[j][index] == colval[i]:
                        d.list_append(j, self.rows_()[j])
        if numrows > 0:
            d = d.get_top_rows(numrows, top=top)
        return d

    def header_(self) -> list:
        return self.header.copy()

    def header_index(self, colKey) -> int:
        try:
            i = self.header.index(colKey)
        except ValueError:
            raise Exceptions.valueNotFound()
        else:
            return i

    def key_(self) -> Union[float, int, None]:
        """
        if datatable has only one row, return the key of its row
        """
        if len(self) == 1:
            return self.row_keys()[0]
        return None

    def list_append(self, key, lst: list) -> None:
        if len(lst) > len(self.header):
            raise Exceptions.wrongArguments(
                "number of keys in the input dict can not be more than the the number of keys in the dataTable")
        if floatable(key) is True:
            if len(self.header) > len(lst):
                temp_var = len(self.header) - len(lst)
                for i in range(temp_var):
                    lst.append(None)
            self.rows[key] = lst.copy()
        else:
            temp_var = str(type(key))[7:-1]
            raise Exceptions.wrongArguments(f"{temp_var} Type was given while float is permitted")

    def merger(self, table: dataTable) -> None:
        """
        adds  cols in table that are not in self to self, non-existing row values should be filled with none
        """
        for head in table.header_():
            if head not in self.header_():
                self.header.append(head)
                for row in self.row_keys():
                    row_lst = self[row]
                    if row not in table.row_keys():
                        row_lst.append(None)
                    else:
                        row_lst.append(table.get_row_cell_value(row, head))
                    self[row] = row_lst
        table_width = self.size_()[1]
        for head in table.header_():
            for row in table.row_keys():
                if row not in self.row_keys():
                    lst = []
                    for i in range(table_width - 1):
                        lst.append(None)
                    lst.append(table.get_row_cell_value(row, head))
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

    def multiply_cols(self, other) -> dataTable:
        """
        perform multipication between two cols
        self and other is a datatable with one column
        """
        if (self.size_()[1] != 1) or (other.size_ != 1):
            raise Exceptions.wrongArguments("multipication is only allowed between two one column dataframe")
        dic = {}
        for key in self:
            dic[key] = self[key][0] * other[key][0]
        d = dataTable(["results"])
        d.dict_append(dic)
        return d

    def divide_cols(self, other):
        """
        perform division between two cols, self/other
        self and other is a datatable with one column
        """
        if (self.size_()[1] != 1) or (other.size_ != 1):
            raise Exceptions.wrongArguments("division is only allowed between two one-column dataframes")
        dic = {}
        for key in self:
            dic[key] = self[key][0] / other[key][0]
        d = dataTable(["result"])
        d.dict_append(dic)
        return d

    def multiply_and_operate(self, factor1: Any, factor2: Any, op: Any, from_: str, operation: str = "minus"):
        """

        factor1 : factor 1 colkey
        factor2 : factor 2 clokey
        op : operation colkey

        Returns:
        from = product :
        (f1 * f2) {operation} op
        from = "factor"
        f1 * (f2 {operation} op)
        """

        if (factor1 not in self.header) or (factor2 not in self.header) or (op not in self.header):
            raise Exceptions.valueNotFound()
        val = 0
        f1 = self.header_index(factor1)
        f2 = self.header_index(factor2)
        m = self.header_index(op)
        if from_ == "product":
            for i in self:
                temp = self.rows[i][f1] * self.rows[i][f2]
                if operation == "minus":
                    val += (temp - self.rows[i][m])
                elif operation == "plus":
                    val += (temp + self.rows[i][m])
                elif operation == "divide":
                    val += (temp / self.rows[i][m])
                elif operation == "multiply":
                    val += (temp * self.rows[i][m])
        elif from_ == "factor":
            for i in self:
                temp = 0
                if operation == "minus":
                    temp = self.rows[i][f2] - self.rows[i][m]
                elif operation == "plus":
                    temp = self.rows[i][f2] + self.rows[i][m]
                elif operation == "divide":
                    temp = self.rows[i][f2] / self.rows[i][m]
                elif operation == "multiply":
                    temp = self.rows[i][f2] * self.rows[i][m]
                val += (self.rows[i][f1] * temp)
        else:
            raise Exceptions.wrongArguments()

        return val

    def rows_(self) -> dict:
        return self.rows.copy()

    def row_keys(self) -> list:
        return list(self.rows.keys())

    def row_replace(self, key: float, value: Any):
        if key in self.rows:
            self.rows[key] = value
        else:
            raise Exceptions.valueNotFound("you should consider appending the data")

    def row_to_dict(self, rowList: list) -> dict:
        rowList = rowList.copy()
        headers = self.header_()
        if len(rowList) < len(headers):
            while len(rowList) < len(headers):
                rowList.append(None)
        elif len(rowList) > len(headers):
            raise Exceptions.wrongArguments("lenght of rowList should not be bigger than dataTable headers")
        dictionary = {}
        for i in range(len(rowList)):
            dictionary[headers[i]] = rowList[i]
        return dictionary.copy()

    def ret(self, colkey: str, inplace: bool = True, inplace_name: str = "ret") -> Union[dataTable, None]:
        colkey_index = self.header_index(colkey)
        dt = dataTable([inplace_name])
        rows = self.row_keys()
        for i in range(1, len(rows)):
            ret = (self[rows[i]][colkey_index] - self[rows[i - 1]][colkey_index]) / self[rows[i - 1]][colkey_index]
            dt.list_append(rows[i], [ret])
        if inplace is False:
            return dt
        self.merger(dt)

    def pct(self, x1: str, x2: str, inplace: bool = False, inplace_name: str = "pct") -> Union[dataTable, None]:
        x1_index = self.header_index(x1)
        x2_index = self.header_index(x2)
        dt = dataTable([inplace_name])
        for row in self.row_keys():
            ret = (self[row][x2_index] - self[row][x1_index]) / self[row][x1_index]
            dt.list_append(row, [ret])
        if inplace is False:
            return dt
        self.merger(dt)

    def set_value_at(self, rowKey: float, colKey: Any, value: Any) -> None:
        row = self[rowKey].copy()
        row[self.header_index(colKey)] = value
        self.row_replace(rowKey, row)

    def size_(self) -> Tuple[int, int]:
        t = tuple([len(self.rows), len(self.header)])
        return t

    def sum_col(self, colKey: Any):
        summ = 0
        try:
            index = self.header_index(colKey)
        except ValueError:
            raise Exceptions.valueNotFound()
        for i in self:
            try:
                summ += float(self.rows[i][index])
            except:
                pass
        return summ

    def sum_cols(self):
        dic = {}
        for i in self.header:
            dic[i] = self.sum_col(i)
        return dic

    def table_duration(self) -> float:
        """
            returns the duration passed between the first entry and the last one
        """
        r = self.row_keys()
        r.sort()
        return r[-1] - r[0]

    def to_pandas(self):
        import pandas
        return pandas.DataFrame.from_dict(self.rows, orient='index', columns=self.header)

    def top_row(self, topp: bool = True) -> Tuple[float, dict]:
        """
        Returns tuple of (key, row)
        """
        if len(self) == 0:
            return tuple([-1, {}])
        t = self.get_top_rows(1, top=topp)
        key = t.row_keys()[0]
        dic = {}
        for i in t.header_():
            dic[i] = t[key][t.header_index(i)]
        return tuple([key, dic])


class dataTable_multi(dataTable):
    def __init__(self, colKeys, directory=None):
        super().__init__(self, colKeys, directory=None)

    def combine_rows(self, key: float, otherRow: list, inplace=False) -> list:
        # other row has to be list of single values and with no tuples
        row = self[key]
        for i in otherRow:
            if isinstance(i, tuple):
                raise Exceptions.wrongArguments("tuples are not permited to be enetered into dataTable by the user")
        final_list = []
        for index, val in enumerate(row):
            if isinstance(val, tuple) is True:
                lst = []
                for i in val:
                    lst.append(i)
                lst.append(otherRow[index])
            else:
                lst = [val, otherRow[index]]
            final_list.append(tuple(lst))

        if inplace is True:
            self.rows[key] = final_list.copy()
            pass
        else:
            return final_list

    def dict_append(self, dic: dict):
        """
        dic = {key: [row]}
        dic = {float: [5, 3], float : [23,"seg"]}
        """

        for date in dic:
            date = float(date)
            for i in dic[date]:
                if isinstance(i, tuple):
                    raise Exceptions.wrongArguments("tuples are not permited to be enetered into dataTable by the user")
            if len(dic[date]) > len(self.header):
                raise Exceptions.wrongArguments(
                    "number of keys in the input dict can not be more than the the number of keys in the dataTable")
            else:
                temp_lst = []
                if len(self.header) > len(dic[date]):
                    temp_var = len(self.header) - len(dic[date])
                    for i in range(0, temp_var):
                        dic[date].append(None)
                if date in list(self.rows.keys()):
                    dic[date] = self.combine_rows(date, dic[date]).copy()
                self.rows[date] = dic[date]

    def dict_extender(self, key: float, dic: dict):
        """
            {str : str, str : str}
            {"ticker":"AVAX-USDT", "side":"sell",
        """
        dic_keys = list(dic.keys())
        lst = []
        for i in self.header:
            if i in dic_keys:
                lst.append(dic[i])
            else:
                lst.append(None)
        self.list_append(key, lst)

    # def get_rows_by_col_value(self, colkey: list, colval: list, numrows=None, top=True):
    #     """
    #         return rows that have the same colvalue at the specified colkey
    #         for multiple colvalues in a sepcific colkey put all the colvals insude a lsit
    #         when multiple values used, it does not search on top of each parameter, the return includes all results
    #     """
    #     if (not isinstance(colkey, list)) or (not isinstance(colval, list)):
    #         temp_var = str(type(colkey))[7:-1]
    #         temp_var1 = str(type(colval))[7:-1]
    #         raise Exceptions.wrongArguments(f"{temp_var}, {temp_var1} Type was given while lists are permitted")
    #     if len(colkey) != len(colval):
    #         raise Exceptions.wrongArguments("colkey and colval should be the same length")
    #     dic = {}
    #     for i in range(len(colkey)):
    #         index = self.header_index(colkey[i])
    #         for j in self:
    #             if isinstance(colval[i], list):
    #                 for k in colval[i]:
    #                     if (isinstance(self[j][index], tuple) is True) and (k in self[j][index]):
    #                         roww = self.rows_()[j]
    #                         final_row = []
    #                         for tup in roww:
    #                             final_row.append(tup[self[j][index].index(k)])
    #                         dic[j] = final_row.copy()
    #                     else:
    #                         if self[j][index] == k:
    #                             if date in list(self.rows.keys()):
    #                                 dic[date] = self.combine_rows(date, dic[date]).copy()
    #                             #if the index already exist then we ned to  ake into the tuple structure
    #                             dic[j] = self.rows_()[j]
    #             else:
    #                 if isinstance(self[j][index], tuple) is True:
    #
    #                 else:
    #                     if (self[j][index] == colval[i]):
    #                         # if the index already exist then we ned to  ake into the tuple structure
    #                         dic[j] = self.rows_()[j]
    #     d = dataTable(self.header_())
    #     d.dict_append(dic)
    #     if numrows is not None:
    #         d = d.get_top_rows(numrows, top=top)
    #     return d

    def list_append(self, date, lst: list):
        if len(lst) > len(self.header):
            raise Exceptions.wrongArguments(
                "number of keys in the input dict can not be more than the the number of keys in the dataTable")
        if isinstance(date, float) or isinstance(date, int):
            for i in lst:
                if isinstance(i, tuple):
                    raise Exceptions.wrongArguments("tuples are not permited to be enetered into dataTable by the user")
            if len(self.header) > len(lst):
                temp_var = len(self.header) - len(lst)
                for i in range(temp_var):
                    lst.append(None)
            if date in list(self.rows.keys()):
                lst = self.combine_rows(date, lst).copy()
            self.rows[date] = lst.copy()
        else:
            temp_var = str(type(date))[7:-1]
            raise Exceptions.wrongArguments(f"{temp_var} Type was given while float is permitted")


def pd_to_dataTable(df) -> dataTable:
    data = df.to_dict(orient="split")
    dt: dataTable = dataTable(data["columns"])
    for index, i in enumerate(data["index"]):
        dt.list_append(i, data["data"][index])
    return dt


def csv_to_dataTable(directory: str, line_seperator: str = ",") -> dataTable:
    """
    the csv file must have a header row and has to have a character or a free space for the index header
    """
    lst = []
    with open(directory, 'r') as file:
        data = file.read().split()
    for line in data:
        if line != "":
            lst.append(line.split(sep=line_seperator))
    dt = dataTable(lst[0][1:])
    for i in range(1, len(lst)):
        key = lst[i][0]
        if floatable(key) is True:
            key = float(key)
        dt.list_append(key, lst[i][1:])
    return dt


# -------------------------------------------------------------------------------


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
# print(f.size_())

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
f = dataTable(["A", "B", "C", "D"])
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

f.set_value_at(st, "B", [34, "XX"])
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
