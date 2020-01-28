from criptomante.model.cotacao import Cotacao
from typing import List

class Sentenca:
    trecho:str
    cotacoes:List[Cotacao]

    def __init__(self):
        self.cotacoes=list()
