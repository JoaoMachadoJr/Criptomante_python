if __name__ == "__main__":
    from dotenv import load_dotenv
    from pathlib import Path
    env_path = Path('C:\\@work\\pessoal\\Criptomante_python\\.env')
    load_dotenv(dotenv_path=env_path)

    import sys
    sys.path.append('C:\\@work\\pessoal\\Criptomante_python')

from criptomante.model.cotacao import Cotacao
from criptomante.repository.cotacoes_repository import CotacoesRepository
from datetime import datetime, timedelta
from criptomante.repository.postagensRepository import PostagensRepository
import re
from criptomante.model.mensagem import Mensagem
from criptomante.model.sentenca import Sentenca
from typing import List
from criptomante.model.frase import Frase
from django.db import transaction


class AnalyzerTextual:
    def analisar(self):
        self.criar_tabela_frases()
        self.criar_tabela_ocorrencia_frases_com_numero_aumentos_e_queda()
        self.criar_tabela_frases_recentes()
        self.criar_tabela_associacao_frases_recentes_com_tabela_de_frases_agrupada_usando_Igualdade_como_criterio()
        self.criar_tabela_associacao_frases_recentes_com_tabela_de_frases_agrupada_usando_linguagem_natural_como_criterio()
        self.preencher_tabela_associacoes_com_ocorrencias()
        self.criar_tabela_resultado_analise_textual() #Essa tabela terá como colunas, o tipo de analise, o resultado e um label.

    def criar_tabela_frases(self):
        BUFFER_MENSAGENS = 10000 #1=88; 10=840; 100=4600; 1000=8000; 10000=5000
        
        repositorio = PostagensRepository()
        feitos = 0        
        while True:
            with transaction.atomic():
                quantidade_total_mensagens = repositorio.quantidade_mensagens_nao_indexadas()
                print("Indexando mensagens. Faltam: {}, feitos:{}".format(quantidade_total_mensagens, feitos))            
                mensagens = repositorio.listarMensagensNaoIndexadas(limite=BUFFER_MENSAGENS)
                if len(mensagens)==0:
                    break
                frases = [frase for m in mensagens
                                for frase in self.gerar_frases(m)]
                repositorio.inserir_tabela_frases(frases)
                repositorio.sinalizar_mensagens_indexadas([m.mensagem for m in mensagens])
                feitos = feitos + len(mensagens)

    def criar_tabela_ocorrencia_frases_com_numero_aumentos_e_queda(self):
        BUFFER_FRASES = 100000

        repositorio = PostagensRepository()
        repositorio_cotacoes = CotacoesRepository()
        cotacoes = repositorio_cotacoes.listarCotacoesComoMapaDeDatas()
        data_limite_de_analise = repositorio_cotacoes.data_ultima_cotacao() - timedelta(hours=24)
        while True:
            quantidade_total_frases = repositorio.quantidade_frases_nao_indexadas(data_limite_de_analise)
            print("Indexando frases. Faltam: {}".format(quantidade_total_frases))
            frases = repositorio.listarFrasesNaoIndexadas(limite=BUFFER_FRASES, data_maxima=data_limite_de_analise)
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
                        resultado[frase.texto]["texto"]=frase.texto
                    variacao = cotacoes[datahora_seguinte];cotacoes[datahora]
                    if variacao<=0.95:
                        resultado[frase.texto]["Queda"] +=1
                    elif variacao>=1.05:
                        resultado[frase.texto]["Aumento"] +=1
                    else:
                        resultado[frase.texto]["Estavel"]+=1
                    frase.indexada=True
                elif (datahora + timedelta(hours=24) < data_limite_de_analise):
                    frase.indexada=True
                else:
                    frase.indexada=False

            repositorio.registrar_ocorrencia_frases_com_numero_aumentos_e_queda(list(resultado.values()))
            repositorio.sinalizar_frases_indexadas([frase.frase for frase in frases if frase.indexada])
        
    def criar_tabela_frases_recentes(self):
        repositorio = PostagensRepository()
        repositorio.criar_tabela_frases_recentes()
    
    def criar_tabela_associacao_frases_recentes_com_tabela_de_frases_agrupada_usando_Igualdade_como_criterio(self):
        repositorio = PostagensRepository()
        frases_recentes = repositorio.listar_frases_recentes()
        frases = repositorio.listar_frases_com_tendencia()
        resultado = list()
        for frase in frases:
            if frase in frases_recentes:
                nova = dict()
                nova["frase_recente"] = frase
                nova["frase"]= frase
                resultado.append(nova)
        repositorio.insere_tabela_associacoes(resultado, "Igualdade")

    def criar_tabela_associacao_frases_recentes_com_tabela_de_frases_agrupada_usando_linguagem_natural_como_criterio(self):
        repositorio = PostagensRepository()
        frases_recentes = repositorio.listar_frases_recentes()
        frases = repositorio.listar_frases_com_tendencia()
        resultado = list()
        for frase in frases:
            frase_equivalente :str= self.equivalencia_semantica(frase, frases_recentes)
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


    def gerar_frases(self, mensagem:Mensagem):
        frases = [re.sub(r"[^a-zA-Z0-9,]+", ' ', s).strip() for s in re.split("\\. |\\? |! ",mensagem.texto)]
        return [Frase(f,mensagem.mensagem) for f in frases if f!="" and not "http" in f]
    
    def equivalencia_semantica(self, frase, frases_recentes):
        return None
        













if __name__ == "__main__":
    a = AnalyzerTextual()
    a.analisar()