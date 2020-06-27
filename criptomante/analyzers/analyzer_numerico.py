if __name__ == "__main__":
    from dotenv import load_dotenv
    from pathlib import Path
    env_path = Path('C:\\@work\\pessoal\\Criptomante_python\\.env')
    load_dotenv(dotenv_path=env_path)

    import sys
    sys.path.append('C:\\@work\\pessoal\\Criptomante_python')
    
from criptomante.repository.cotacoes_repository import CotacoesRepository
from typing import List, Dict
from criptomante.model.cotacao import Cotacao
from criptomante.model.snapshot import Snapshot
from datetime import datetime
from decimal import Decimal
from criptomante.repository.resultados_repository import ResultadosRepository
from statistics import median


class AnalizerNumerico:
    def analisar(self, pessimismo = 0.7):
        repositorio = CotacoesRepository()                
        cotacoes = repositorio.listarCotacoes()  
        cotacoes = self.agrupar_cotacoes_por_dia(cotacoes)      
        snapshots = self.calcular_snapshots(cotacoes)
        ultimo_snapshot = self.ultimo_snapshot(snapshots)
        self.pontuar_snapshots(snapshots, ultimo_snapshot)
        ResultadosRepository().inserir_snapshots(snapshots)
        resultado_por_media = self.prever_por_media(ultimo_snapshot, snapshots)
        repositorio.gravarAnaliseNumerica(resultado_por_media, "media")
        resultado_por_amostra = self.prever_por_amostra(ultimo_snapshot, snapshots, pessimismo)        
        repositorio.gravarAnaliseNumerica(resultado_por_amostra, "amostragem")
        resultado_por_mediana = self.prever_por_mediana(ultimo_snapshot, snapshots)        
        repositorio.gravarAnaliseNumerica(resultado_por_mediana, "mediana")

    def calcular_snapshots(self, cotacoes:List[Cotacao]):
        print("calcular_snapshots")
        cotacoes_dicionario :Dict[datetime, Cotacao] = dict()
        for c in cotacoes:
            cotacoes_dicionario[c.data] = c

        snapshots =  [Snapshot(c) for c in cotacoes]                
        
        for s in snapshots:
            for key in s.momentos:
                if (s.momentos[key].data in cotacoes_dicionario) and (s.momentos[key].data_referencia in cotacoes_dicionario):
                    momento = s.momentos[key]
                    momento.valor = cotacoes_dicionario[momento.data].valor
                    momento.variacao = (cotacoes_dicionario[momento.data].valor - cotacoes_dicionario[momento.data_referencia].valor)/(cotacoes_dicionario[momento.data_referencia].valor)
                else:
                    s.momentos[key].valor=0
                    s.momentos[key].variacao=0

        snapshots.sort(key=lambda x: x.data)
        return snapshots
    
    def ultimo_snapshot(self, snapshots):
        return snapshots[len(snapshots)-1]

    def pontuar_snapshots(self, snapshots:List[Snapshot], ultimo:Snapshot):
        print("pontuar_snapshots")        
        for s in snapshots:
            pontuacao=0
            for key in ultimo.momentos_passado():
                if s.momentos[key].valor!=0:
                    semelhanca = abs(s.momentos[key].variacao/ultimo.momentos[key].variacao)
                    if (semelhanca>0.9 and semelhanca<1.1):
                        pontuacao=pontuacao+1
            s.pontuacao=pontuacao

    def prever_por_media(self, ultimo, snapshots):
        print("prever_por_media")        
        for key in ultimo.momentos_futuro():
            dividendo = 0
            divisor = 0
            for s in snapshots:
                if (s.momentos[key].valor!=0) and (s.pontuacao!=0) and (s.data!=ultimo.data):
                    dividendo += Decimal(s.momentos[key].variacao) * Decimal(pow(s.pontuacao, s.pontuacao))
                    divisor += (pow(s.pontuacao, s.pontuacao))
            ultimo.momentos[key].variacao = dividendo/divisor
            ultimo.momentos[key].valor = ultimo.momentos["DR"].valor*(1+ultimo.momentos[key].variacao)
        lista_auxiliar = sorted(snapshots, key=lambda x: x.pontuacao, reverse=True)
        return ultimo

    def prever_por_amostra(self, ultimo, snapshots, pessimismo):
        print("prever_por_amostra")        
        for key in ultimo.momentos_futuro():
            variacoes = list()
            for s in snapshots:
                if (s.momentos[key].valor!=0) and (s.pontuacao!=0) and (s.data!=ultimo.data):
                    variacoes += [s.momentos[key].variacao for x in range(pow(s.pontuacao, s.pontuacao))]
            variacoes.sort(reverse=True)
            indice_com_pessimismo = min(int(len(variacoes)*pessimismo), len(variacoes)-1)
            ultimo.momentos[key].variacao = variacoes[indice_com_pessimismo]      
            ultimo.momentos[key].valor =  ultimo.momentos["DR"].valor*(1+ultimo.momentos[key].variacao)
            
        return ultimo

    def prever_por_mediana(self, ultimo, snapshots):
        print("prever_por_mediana")        
        for key in ultimo.momentos_futuro():
            variacoes = list()
            for s in snapshots:
                if (s.momentos[key].valor!=0) and (s.pontuacao!=0) and (s.data!=ultimo.data):
                    variacoes += [s.momentos[key].variacao for x in range(pow(s.pontuacao, s.pontuacao))]
            ultimo.momentos[key].variacao = median(variacoes)
            ultimo.momentos[key].valor =  ultimo.momentos["DR"].valor*(1+ultimo.momentos[key].variacao)            
        return ultimo
    
    def agrupar_cotacoes_por_dia(self, cotacoes:List[Cotacao]):
        print("agrupar_cotacoes_por_dia")
        for c in cotacoes:
            c.data = c.data.replace(hour=0)
        return Cotacao.agrupar(cotacoes)

if __name__ == "__main__":
    a = AnalizerNumerico()
    a.analisar()
    



    






