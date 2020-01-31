from threading import Thread
from criptomante.util.thread_util import ThreadUtil

from datetime import datetime

class MinhaThread(Thread):
    esperando_novos = False
    quantidade_limite = 100 

    @property
    def continuar(self):
        return self._continuar
    
    @continuar.setter
    def continuar(self, value):
        self._continuar=value

    def run(self):
        self.name = self.__class__.__name__
        self.continuar = True
        self.hora_inicio = datetime.now()
        while (self.continuar):
            try:
                self.executar()
            except Exception as e:
                self.mensagem_erro(e)
    
    def mensagem_erro(self, e):
        print("Erro: {}".format(e))
    
    def parar(self):
        self.continuar=False
    
    def executar(self):
        pass

    @classmethod
    def fabricar(cls, quantidade, offset = 0):
        return list()


