from dateutil.relativedelta import relativedelta
from datetime import datetime
from typing import Dict
from criptomante.model.cotacao import Cotacao

class Momento:
    data: datetime
    data_referencia: datetime
    valor: float
    variacao: float
    

    def __init__(self, data_atual : datetime= None, intervalo_da_data_atual: relativedelta= None, intervalo_da_data_referencia: relativedelta= None):        
        if data_atual != None:
            self.data = data_atual + intervalo_da_data_atual
            self.data_referencia = self.data + intervalo_da_data_referencia
    

    @staticmethod
    def encode(data, data_referencia):
        m = Momento()
        m.data=data
        m.data_referencia =data_referencia
        return m 


class Snapshot:
    momentos:Dict[str, Momento]
    data:datetime
    pontuacao:float
    ccotacao=0
        
    def __init__(self, cotacao = None):
        self.momentos=dict()
        if cotacao!=None:
            self.cotacao=cotacao
        

    @property
    def cotacao(self):
        return self.ccotacao
    
    @cotacao.setter
    def cotacao(self, value):
        self.ccotacao = value
        self.data = self.cotacao.data
        self.momentos["DR-6M"]=Momento(value.data, relativedelta(months=-6), relativedelta(months=-6))
        self.momentos["DR-5M"]=Momento(value.data, relativedelta(months=-5), relativedelta(months=-1))
        self.momentos["DR-4M"]=Momento(value.data, relativedelta(months=-4), relativedelta(months=-1))
        self.momentos["DR-3M"]=Momento(value.data, relativedelta(months=-3), relativedelta(months=-1))
        self.momentos["DR-2M"]=Momento(value.data, relativedelta(months=-2), relativedelta(months=-1))
        self.momentos["DR-1M"]=Momento(value.data, relativedelta(months=-1), relativedelta(months=-1))
        self.momentos["DR-25D"]=Momento(value.data, relativedelta(days=-25), relativedelta(days=-5))
        self.momentos["DR-20D"]=Momento(value.data, relativedelta(days=-20), relativedelta(days=-5))
        self.momentos["DR-15D"]=Momento(value.data, relativedelta(days=-15), relativedelta(days=-5))
        self.momentos["DR-10D"]=Momento(value.data, relativedelta(days=-10), relativedelta(days=-5))
        self.momentos["DR-5D"]=Momento(value.data, relativedelta(days=-5), relativedelta(days=-5))
        self.momentos["DR-1D"]=Momento(value.data, relativedelta(days=-1), relativedelta(days=-4))
        self.momentos["DR"]=Momento(value.data, relativedelta(days=-0), relativedelta(days=-1))
        self.momentos["DR+1D"]=Momento(value.data, relativedelta(days=1), relativedelta(days=-1))
        self.momentos["DR+5D"]=Momento(value.data, relativedelta(days=5), relativedelta(days=-5))
        self.momentos["DR+10D"]=Momento(value.data, relativedelta(days=10), relativedelta(days=-10))
        self.momentos["DR+15D"]=Momento(value.data, relativedelta(days=15), relativedelta(days=-15))
        self.momentos["DR+20D"]=Momento(value.data, relativedelta(days=20), relativedelta(days=-20))
        self.momentos["DR+25D"]=Momento(value.data, relativedelta(days=25), relativedelta(days=-25))
        self.momentos["DR+1M"]=Momento(value.data, relativedelta(months=1), relativedelta(months=-1))
        self.momentos["DR+2M"]=Momento(value.data, relativedelta(months=2), relativedelta(months=-2))
        self.momentos["DR+3M"]=Momento(value.data, relativedelta(months=3), relativedelta(months=-3))
        self.momentos["DR+4M"]=Momento(value.data, relativedelta(months=4), relativedelta(months=-4))
        self.momentos["DR+5M"]=Momento(value.data, relativedelta(months=5), relativedelta(months=-5))
        self.momentos["DR+6M"]=Momento(value.data, relativedelta(months=6), relativedelta(months=-6))
        self.momentos["DR+12M"]=Momento(value.data, relativedelta(months=12), relativedelta(months=-12))

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
    

    
    
    
    
    
    
    
    
    
    
    