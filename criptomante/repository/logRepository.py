from criptomante.repository.abstract_repository import AbstractRepository
import traceback
import threading
from criptomante.util.json_util import JsonUtil
class LogRepository(AbstractRepository):
    def insere(self, texto):
        sql = "insert into log (texto, trace, thread) values (:texto, :trace, :thread)"
        params = dict()
        params["texto"] = texto
        params["trace"] = JsonUtil().encode(traceback.extract_stack())
        params["thread"] = threading.current_thread().name
        self.execute(sql, params)