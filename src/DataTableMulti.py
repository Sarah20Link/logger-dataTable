from __future__ import annotations
from typing import (Any, Tuple, Union)
from exceptions import wrongArguments, valueNotFound
from DataTable import DataTable
"""
Created on Tue Dec 21 00:36:25 2021

@author: sarah
"""

__all__ = ["DataTableMulti"]


class DataTableMulti(DataTable):
    def __init__(self, colKeys, directory=None):
        super().__init__(self, colKeys, directory=None)

    def combine_rows(self, key: float, otherRow: list, inplace=False) -> list:
        # other row has to be list of single values and with no tuples
        row = self[key]
        for i in otherRow:
            if isinstance(i, tuple):
                raise wrongArguments("tuples are not permited to be enetered into dataTable by the user")
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
                    raise wrongArguments("tuples are not permited to be enetered into dataTable by the user")
            if len(dic[date]) > len(self.header):
                raise wrongArguments(
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
    #         raise wrongArguments(f"{temp_var}, {temp_var1} Type was given while lists are permitted")
    #     if len(colkey) != len(colval):
    #         raise wrongArguments("colkey and colval should be the same length")
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
            raise wrongArguments(
                "number of keys in the input dict can not be more than the the number of keys in the dataTable")
        if isinstance(date, float) or isinstance(date, int):
            for i in lst:
                if isinstance(i, tuple):
                    raise wrongArguments("tuples are not permited to be enetered into dataTable by the user")
            if len(self.header) > len(lst):
                temp_var = len(self.header) - len(lst)
                for i in range(temp_var):
                    lst.append(None)
            if date in list(self.rows.keys()):
                lst = self.combine_rows(date, lst).copy()
            self.rows[date] = lst.copy()
        else:
            temp_var = str(type(date))[7:-1]
            raise wrongArguments(f"{temp_var} Type was given while float is permitted")

