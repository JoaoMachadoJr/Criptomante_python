from dateutil.relativedelta import relativedelta
from datetime import datetime
from typing import Dict
from criptomante.model.cotacao import Cotacao

class Momento:
    data: datetime
    data_referencia: datetime
    valor: float
    variacao: float
    

    def __init__(self, data_atual : datetime, intervalo_da_data_atual: relativedelta, intervalo_da_data_referencia: relativedelta):        
        self.data = data_atual + intervalo_da_data_atual
        self.data_referencia = self.data + intervalo_da_data_referencia


class Snapshot:
    momentos:Dict[str, Momento]
    data:datetime
    pontuacao:float
        
    def __init__(self, cotacao):
        self.momentos=dict()
        self.cotacao=cotacao
        

    @property
    def cotacao(self):
        return self._cotacao
    
    @cotacao.setter
    def cotacao(self, value):
        self._cotacao = value
        self.data = self.cotacao.data
        self.momentos["menos_6m"]=Momento(value.data, relativedelta(months=-6), relativedelta(months=-6))
        self.momentos["menos_5m"]=Momento(value.data, relativedelta(months=-5), relativedelta(months=-1))
        self.momentos["menos_4m"]=Momento(value.data, relativedelta(months=-4), relativedelta(months=-1))
        self.momentos["menos_3m"]=Momento(value.data, relativedelta(months=-3), relativedelta(months=-1))
        self.momentos["menos_2m"]=Momento(value.data, relativedelta(months=-2), relativedelta(months=-1))
        self.momentos["menos_1m"]=Momento(value.data, relativedelta(months=-1), relativedelta(months=-1))
        self.momentos["menos_25d"]=Momento(value.data, relativedelta(days=-25), relativedelta(days=-5))
        self.momentos["menos_20d"]=Momento(value.data, relativedelta(days=-20), relativedelta(days=-5))
        self.momentos["menos_15d"]=Momento(value.data, relativedelta(days=-15), relativedelta(days=-5))
        self.momentos["menos_10d"]=Momento(value.data, relativedelta(days=-10), relativedelta(days=-5))
        self.momentos["menos_5d"]=Momento(value.data, relativedelta(days=-5), relativedelta(days=-5))
        self.momentos["menos_1d"]=Momento(value.data, relativedelta(days=-1), relativedelta(days=-4))
        self.momentos["atual"]=Momento(value.data, relativedelta(days=-0), relativedelta(days=-1))
        self.momentos["mais_1d"]=Momento(value.data, relativedelta(days=1), relativedelta(days=-1))
        self.momentos["mais_5d"]=Momento(value.data, relativedelta(days=5), relativedelta(days=-5))
        self.momentos["mais_10d"]=Momento(value.data, relativedelta(days=10), relativedelta(days=-10))
        self.momentos["mais_15d"]=Momento(value.data, relativedelta(days=15), relativedelta(days=-15))
        self.momentos["mais_20d"]=Momento(value.data, relativedelta(days=20), relativedelta(days=-20))
        self.momentos["mais_25d"]=Momento(value.data, relativedelta(days=25), relativedelta(days=-25))
        self.momentos["mais_1m"]=Momento(value.data, relativedelta(months=1), relativedelta(months=-1))
        self.momentos["mais_2m"]=Momento(value.data, relativedelta(months=2), relativedelta(months=-2))
        self.momentos["mais_3m"]=Momento(value.data, relativedelta(months=3), relativedelta(months=-3))
        self.momentos["mais_4m"]=Momento(value.data, relativedelta(months=4), relativedelta(months=-4))
        self.momentos["mais_5m"]=Momento(value.data, relativedelta(months=5), relativedelta(months=-5))
        self.momentos["mais_6m"]=Momento(value.data, relativedelta(months=6), relativedelta(months=-6))
        self.momentos["mais_12m"]=Momento(value.data, relativedelta(months=12), relativedelta(months=-12))

    def momentos_passado(self):
        saida = dict()
        for key in self.momentos:
            if self.momentos[key].data<=self.data:
                saida[key]=self.momentos[key]
        return saida

    def momentos_futuro(self):
        saida = dict()
        for key in self.momentos:
            if self.momentos[key].data>self.data:
                saida[key]=self.momentos[key]
        return saida
    

    
    
    
    
    
    
    
    
    
    
    