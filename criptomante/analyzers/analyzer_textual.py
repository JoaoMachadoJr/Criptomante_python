from criptomante.model.cotacao import Cotacao
from criptomante.repository.cotacoes_repository import CotacoesRepository
from datetime import datetime, timedelta
from criptomante.repository.postagensRepository import PostagensRepository
import re
from criptomante.model.mensagem import Mensagem
from criptomante.model.sentenca import Sentenca
from typing import List

class AnalyzerTextual:
    def analizar2(self):
        self.criar_tabela_frases()
        self.criar_tabela_ocorrencia_frases_com_numero_aumentos_e_queda()
        self.criar_tabela_frases_recentes()
        self.criar_tabela_associacao_frases_recentes_com_tabela_de_frases_agrupada_usando_Igualdade_como_criterio()
        self.criar_tabela_associacao_frases_recentes_com_tabela_de_frases_agrupada_usando_linguagem_natural_como_criterio()
        self.preencher_tabela_associacoes_com_ocorrencias()
        self.criar_tabela_resultado_analise_textual() #Essa tabela terá como colunas, o tipo de analise, o resultado e um label.

    def criar_tabela_frases(self):
        BUFFER_MENSAGENS = 100000
        
        repositorio = PostagensRepository()        
        tratadas = 0
        while True:
            quantidade_total_mensagens = repositorio.quantidade_mensagens_nao_indexadas()
            print("Indexando mensagens. Faltam: {}".format(quantidade_total_mensagens))            
            mensagens = repositorio.listarMensagensNaoIndexadas(limite=BUFFER_MENSAGENS)
            if len(mensagens)==0:
                break
            frases = [frase for m in mensagens
                            for frase in self.gerarFrases(m)]
            repositorio.inserir_tabela_frases(frases)
            repositorio.sinalizar_mensagens_indexadas([m.mensagem for m in mensagens])

    def criar_tabela_ocorrencia_frases_com_numero_aumentos_e_queda(self):
        BUFFER_FRASES = 100000

        repositorio = PostagensRepository()
        repositorio_cotacoes = CotacoesRepository()
        cotacoes = repositorio_cotacoes.listarCotacoesComoMapaDeDatas()
        data_limite_de_analise = repositorio_cotacoes.data_ultima_cotacao() - timedelta(hours=24)
        while True:
            quantidade_total_frases = repositorio.quantidade_frases_nao_indexadas(data_limite_de_analise)
            print("Indexando frases. Faltam: {}".format(quantidade_total_frases))
            frases = repositorio.listarFrases(limite=BUFFER_FRASES, data_maxima=data_limite_de_analise)
            if len(frases)==0:
                break
            resultado = dict()            
            for frase in frases:
                datahora = frase.data.replace(minute=0, second=0, microsecond=0)
                datahora_seguinte = datahora + timedelta(hours=24)
                if (datahora in cotacoes) and (datahora_seguinte in cotacoes):
                    if not frase.texto in resultado:
                        resultado[frase.texto] = dict()
                        resultado[frase.texto]["Queda"]=0
                        resultado[frase.texto]["Aumento"]=0
                        resultado[frase.texto]["Estavel"]=0
                    variacao = cotacoes[datahora_seguinte];cotacoes[datahora]
                    if variacao<=0.95:
                        resultado[frase.texto]["Queda"] +=1
                    elif variacao>=1.05:
                        resultado[frase.texto]["Aumento"] +=1
                    else:
                        resultado[frase.texto]["Estavel"]+=1
                    frase.indexada=True
                elif (datahora + timedelta(hours24) < data_limite_de_analise):
                    frase.indexada=True
                else:
                    frase.indexada=False

            repositorio.registrar_ocorrencia_frases_com_numero_aumentos_e_queda(resultado)
            repositorio.sinalizar_frases_indexadas([frase.texto for frase in frases if frase.indexada])
        
    def criar_tabela_frases_recentes(self):
        repositorio = PostagensRepository()
        repositorio.criar_tabela_postagens_recentes()
    
    def criar_tabela_associacao_frases_recentes_com_tabela_de_frases_agrupada_usando_Igualdade_como_criterio(self):
        BUFFER_FRASES = 100000

        repositorio = PostagensRepository()
        frases_recentes = repositorio.listar_frases_recentes()
        frases = repositorio.listar_frases_com_tendencia()
        resultado = list()
        for frase in frases:
            if frase.texto in frases_recentes:
                nova = dict()
                nova["frase_recente"] = frase.texto
                nova["frase"]= frase.texto
                resultado.append(nova)
        repositorio.insere_tabela_associacoes(resultado, "Igualdade")

    def criar_tabela_associacao_frases_recentes_com_tabela_de_frases_agrupada_usando_linguagem_natural_como_criterio(self):
        BUFFER_FRASES = 100000

        repositorio = PostagensRepository()
        frases_recentes = repositorio.listar_frases_recentes()
        frases = repositorio.listar_frases_com_tendencia()
        resultado = list()
        for frase in frases:
            frase_equivalente = self.equivalencia_semantica(frase, frases_recentes)
            if frase_equivalente!=None:
                nova = dict()
                nova["frase_recente"] = frase_equivalente
                nova["frase"]= frase.texto
                resultado.append(nova)
        repositorio.insere_tabela_associacoes(resultado, "Equivalencia Semantica")
    
    def preencher_tabela_associacoes_com_ocorrencias(self):
        repositorio = PostagensRepository()
        repositorio.preencher_tabela_associacoes_com_ocorrencias()

    def criar_tabela_resultado_analise_textual(self):
        repositorio = PostagensRepository()
        associacoes_igualdade = repositorio.listarAssociacoes("Igualdade")
        associacoes_semantica = repositorio.listarAssociacoes("Equivalencia Semantica")
        
        resultado = dict()
        resultado["aumento"]=0
        resultado["queda"]=0
        
        for a in associacoes_igualdade:
            resultado[a["tendencia"]]+=1
        repositorio.insere_resultado_analise_textual(resultado, "igualdade")


        resultado = dict()
        resultado["aumento"]=0
        resultado["queda"]=0
        
        for a in associacoes_semantica:
            resultado[a["tendencia"]]+=1
        repositorio.insere_resultado_analise_textual(resultado, "Equivalencia Semantica")














                
            

        






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