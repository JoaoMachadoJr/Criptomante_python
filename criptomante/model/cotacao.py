from datetime import datetime
from typing import List


class Cotacao:
    data:datetime
    valor:float
    transacoes:int

    def somar_valor(self, valor, transacoes = 1):
        self.valor = ((self.valor*self.transacoes) + (valor*transacoes)) / (self.transacoes+transacoes)
        self.transacoes = self.transacoes+transacoes

    @classmethod
    def agrupar(cls, cotacoes:list):
        mapa = dict()
        for cotacao in cotacoes:
            cotacao.data = cotacao.data.replace(minute=0,second=0,microsecond=0,tzinfo=None)
            if cotacao.data in mapa:
                mapa[cotacao.data].somar_valor(cotacao.valor, cotacao.transacoes)
            else:
                mapa[cotacao.data] = cotacao
        return sorted(list(mapa.values()), key=lambda x: x.data)
        
