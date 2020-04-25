from typing import List
from django.db import connections, transaction, connection
from criptomante.util.cursor_util import CursorUtil
from enum import Enum
import numpy

class AbstractRepository:


    def begin(self):
        transaction.set_autocommit(False)        

    def commit(self):
        transaction.commit()
        transaction.set_autocommit(True)
        connection.close()

    def rollback(self):
        transaction.rollback()
        transaction.set_autocommit(True)

    def execute(self, sql: str, params: dict = dict()):
        with connection.cursor() as cursor:
            self.execute_retornando_cursor(sql, params, cursor)
        if transaction.get_autocommit():
            connection.close()
    
    def conexao(self):
        return transaction.get_connection()

        
        
    def executeMany(self, sql, params:list):
        if len(params)==0:
            return
        from sqlparams import SQLParams
        sql2, params2 = SQLParams('named', 'format').formatmany(sql, params)
        with connection.cursor() as cursor:
            cursor.executemany(sql2,params2)

    def fetchAll(self, sql: str, params: dict = dict()) -> List[dict]:
        with connection.cursor() as cursor:
            self.execute_retornando_cursor(sql, params, cursor)
            retorno = CursorUtil().fetchall(cursor)
        if transaction.get_autocommit():
            connection.close()
        return retorno

    def fetchOne(self, sql: str, params: dict = dict()) -> List[dict]:
        with connection.cursor() as cursor:
            self.execute_retornando_cursor(sql, params, cursor)
            retorno = CursorUtil().fetchone(cursor)
        if transaction.get_autocommit():
            connection.close()
        return retorno

    def execute_retornando_cursor(self, sql: str, params: dict, cursor):
        
        from sqlparams import SQLParams
        if not isinstance(params, dict):
            params = params.__dict__
        sql2, params2 = SQLParams('named', 'format').format(sql, params)
        params2 = [elem.value if isinstance(
            elem, Enum) else elem for elem in params2]
        params2 = [int(elem) if isinstance(elem, numpy.int64) else elem for elem in params2]
        cursor.execute(sql2, params2)
        return cursor
