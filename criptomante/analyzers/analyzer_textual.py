from criptomante.model.cotacao import Cotacao
from criptomante.repository.cotacoes_repository import CotacoesRepository
from datetime import datetime, timedelta
from criptomante.repository.postagensRepository import PostagensRepository
import re
from criptomante.model.mensagem import Mensagem
from criptomante.model.sentenca import Sentenca
from typing import List

class AnalyzerTextual:
    def analisar(self):        
        repositorio = CotacoesRepository()
        repositorio_postagens = PostagensRepository()

        mensagens = repositorio_postagens.listarMensagens()
        mensagens_recentes = repositorio_postagens.listarMensagensRecentes() 
        cotacoes = repositorio.listarCotacoes()        
        mapa_cotacoes = {c.data:c for c in cotacoes}
        map_sentencas_recentes = self.agrupar_sentencas_com_cotacoes(mensagens_recentes, mapa_cotacoes, False)


        self.remove_cotacoes_sem_dia_seguinte(mapa_cotacoes)
        map_sentencas_com_cotacoes = self.agrupar_sentencas_com_cotacoes(mensagens, mapa_cotacoes)


        self.filtrar_sentencas_por_igualdade(map_sentencas_com_cotacoes, map_sentencas_recentes)
        print(self.variacao_media(map_sentencas_com_cotacoes))
        



    def remove_cotacoes_sem_dia_seguinte(self, mapa_cotacoes):
        for data in mapa_cotacoes:
            if (data+ timedelta(hours=24)) in mapa_cotacoes:
                mapa_cotacoes[data].dia_seguinte = mapa_cotacoes[data+ timedelta(hours=24)]
            else:
                mapa_cotacoes[data].a_remover = True
        for data in mapa_cotacoes:
            if mapa_cotacoes[data].a_remover:
                del(mapa_cotacoes[data])

    def variacao_media(self, mapa_sentencas):
        dividendo = 0
        divisor = 0
        for s in mapa_sentencas:
            for c in s.cotacoes:
                dividendo+= (c.dia_seguinte.valor - c.valor)/c.valor
                divisor+=1
        return (dividendo/divisor)
    
    def agrupar_sentencas_com_cotacoes(self, mensagens:List[Mensagem], mapa_cotacoes, permite_mensagem_sem_cotacao = True):
        saida = dict()
        for m in mensagens:
            m.data = m.data.replace(minute=0, second=0, microsecond=0)
            if m.data in mapa_cotacoes:
                trechos = self.criar_trecho(m.texto)
                for trecho in trechos:
                    if not trecho in saida:
                        saida[trecho] = Sentenca()
                        saida[trecho].trecho = trecho
                    saida[trecho].cotacoes.append(mapa_cotacoes[m.data])
            else:
                if not permite_mensagem_sem_cotacao:
                    raise Exception("Sem cotacao para: "+str(m.data))
        return saida

    def filtrar_sentencas_por_igualdade(self, map_sentencas_com_cotacoes, map_sentencas_recentes):
        for s1 in map_sentencas_com_cotacoes:
            if not s1 in map_sentencas_recentes:
                del(map_sentencas_com_cotacoes[s1])




    

    def criar_trecho(self, mensagem:str):
        return re.split("(\\. |, |\\? )",mensagem)


if __name__ == "__main__":
    a = AnalyzerTextual()
    a.analisar()