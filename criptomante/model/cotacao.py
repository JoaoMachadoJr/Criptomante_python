from datetime import datetime
from typing import List
import pytz

class Cotacao:
    data:datetime
    valor:float
    transacoes:int
    volume:float

    def somar_valor(self, valor, volume, transacoes):
        if volume!=0:
            self.valor = ((self.valor*self.volume) + (valor*volume)) / (self.volume+volume)
        self.volume = self.volume+volume
        self.transacoes = self.transacoes+transacoes

    @classmethod
    def agrupar(cls, cotacoes:list):
        mapa = dict()
        for cotacao in cotacoes:
            cotacao.data = cotacao.data.replace(minute=0,second=0,microsecond=0)
            if cotacao.data in mapa:
                mapa[cotacao.data].somar_valor(cotacao.valor, cotacao.volume, cotacao.transacoes)
            else:
                mapa[cotacao.data] = cotacao
        return sorted(list(mapa.values()), key=lambda x: x.data)
        
