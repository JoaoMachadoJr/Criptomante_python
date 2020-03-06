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
import  en_core_web_sm, en_core_web_lg
import spacy
from collections import OrderedDict
from criptomante_crawler.threads.thread_generica import ThreadsGenericas
import numpy as np
import string
import pandas as pd
import pandas.io.sql as sqlio
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
from sklearn.base import TransformerMixin
from sklearn.pipeline import Pipeline





class AnalyzerTextual:

    nlp = en_core_web_sm.load()
    

    def analisar(self):
        #self.criar_tabela_frases()
        #self.criar_tabela_ocorrencia_frases_com_numero_aumentos_e_queda()
        #self.preencher_texto_tratado()
        self.criar_tabela_frases_recentes()
        #self.criar_tabela_associacao_frases_recentes_com_tabela_de_frases_agrupada_usando_Igualdade_como_criterio()
        #self.criar_tabela_associacao_frases_recentes_com_tabela_de_frases_agrupada_usando_linguagem_natural_como_criterio()
        #self.preencher_tabela_associacoes_com_ocorrencias()
        #self.criar_tabela_resultado_analise_textual() #Essa tabela terá como colunas, o tipo de analise, o resultado e um label.
        self.carregar_dataset()

    

    def criar_tabela_frases(self):
        
        print("criar_tabela_frases")
        BUFFER_MENSAGENS = 50000 #1=88; 10=840; 100=4600; 1000=8000; 10000=5000
        
        repositorio = PostagensRepository()
        feitos = 0        
        while True:
            with transaction.atomic():
                quantidade_total_mensagens = repositorio.quantidade_mensagens_nao_indexadas()
                print("Indexando mensagens. Faltam: {}, feitos:{}".format(quantidade_total_mensagens, feitos))            
                mensagens = repositorio.listarMensagensNaoIndexadas(limite=BUFFER_MENSAGENS)
                if len(mensagens)==0:
         
                    break
                print("Montando frases")
                frases=list()
                for m in np.array_split(mensagens, 6):
                    ThreadsGenericas.construir(self.gerar_frases, m) 
                frases = [frase for retorno in ThreadsGenericas.resultado()
                                for frase in retorno]
                print("Inserindo frases")
                repositorio.inserir_tabela_frases(frases)
                repositorio.sinalizar_mensagens_indexadas([m.mensagem for m in mensagens])
                feitos = feitos + len(mensagens)

    def preencher_texto_tratado(self):
        print("preencher_texto_tratado")
        BUFFER_FRASES = 10000 
        
        repositorio = PostagensRepository()
        feitos = 0        
        while True:
            quantidade_total_frases = repositorio.quantidade_frases_nao_tratadas()
            print("tratando frases. Faltam: {}, feitos:{}".format(quantidade_total_frases, feitos))            
            frases = repositorio.listarFrasesNaoTratadas(limite=BUFFER_FRASES)
            if len(frases)==0:        
                break
            print("Montando frases")
            for m in np.array_split(frases, 6):
                ThreadsGenericas.construir(self.tratar_frases, m)
            ThreadsGenericas.resultado()                               
            feitos = feitos + len(frases)
        

    def criar_tabela_ocorrencia_frases_com_numero_aumentos_e_queda(self):
        print("criar_tabela_ocorrencia_frases_com_numero_aumentos_e_queda")
        BUFFER_FRASES = 100000

        repositorio = PostagensRepository()
        repositorio_cotacoes = CotacoesRepository()
        cotacoes = repositorio_cotacoes.listarCotacoesComoMapaDeDatas()
        data_limite_de_analise = repositorio_cotacoes.data_ultima_cotacao() - timedelta(hours=24)
        while True:
            with transaction.atomic():
                quantidade_total_frases = repositorio.quantidade_frases_nao_indexadas(data_limite_de_analise)
                print("Indexando frases. Faltam: {}".format(quantidade_total_frases))
                frases = repositorio.listarFrasesNaoIndexadas(limite=BUFFER_FRASES, data_maxima=data_limite_de_analise)
                if len(frases)==0:
                    break
                resultado = dict()            
                for frase in frases:
                    datahora = frase.data_mensagem.replace(minute=0, second=0, microsecond=0)
                    datahora_seguinte = datahora + timedelta(hours=24)
                    if (datahora in cotacoes) and (datahora_seguinte in cotacoes):
                        if not frase.texto in resultado:
                            resultado[frase.texto] = dict()
                            resultado[frase.texto]["Queda"]=0
                            resultado[frase.texto]["Aumento"]=0
                            resultado[frase.texto]["Estavel"]=0
                            resultado[frase.texto]["texto"]=frase.texto
                        variacao = cotacoes[datahora_seguinte].valor/cotacoes[datahora].valor
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

    def criar_tabela_ocorrencia_frases_com_numero_aumentos_e_quedaV2(self):
        print("criar_tabela_ocorrencia_frases_com_numero_aumentos_e_quedaV2")
        BUFFER_FRASES = 10000

        repositorio = PostagensRepository()
        repositorio_cotacoes = CotacoesRepository()
        cotacoes = repositorio_cotacoes.listarCotacoesComoMapaDeDatas()
        data_limite_de_analise = repositorio_cotacoes.data_ultima_cotacao() - timedelta(hours=24)
        quantidade_total_frases = repositorio.quantidade_frases_nao_indexadas(data_limite_de_analise)
        print("Indexando frases. Faltam: {}".format(quantidade_total_frases))
        frases = repositorio.listarFrasesNaoIndexadas(limite=1000000, data_maxima=data_limite_de_analise)
        resultado = dict()
        print("Organizando frases...")            
        for frase in frases:
            datahora = frase.data_mensagem.replace(minute=0, second=0, microsecond=0)
            datahora_seguinte = datahora + timedelta(hours=24)
            if (datahora in cotacoes) and (datahora_seguinte in cotacoes):
                if not frase.texto in resultado:
                    resultado[frase.texto] = dict()
                    resultado[frase.texto]["Queda"]=0
                    resultado[frase.texto]["Aumento"]=0
                    resultado[frase.texto]["Estavel"]=0
                    resultado[frase.texto]["texto"]=frase.texto
                variacao = cotacoes[datahora_seguinte].valor/cotacoes[datahora].valor
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
        print("Inserindo frases")
        
        with transaction.atomic():
            while len(resultado)>0:
                inserir=dict()
                chaves = list(resultado.keys())[0:BUFFER_FRASES]
                for chave in chaves:
                    inserir[chave] = resultado.pop(chave)
                    print("Faltam {}".format(len(resultado)))
                repositorio.registrar_ocorrencia_frases_com_numero_aumentos_e_queda(list(inserir.values()))
            print("Sinalizando indexacao")
            repositorio.sinalizar_frases_indexadas([frase.frase for frase in frases if frase.indexada])



        
    def criar_tabela_frases_recentes(self):
        print("criar_tabela_frases_recentes")
        repositorio = PostagensRepository()
        repositorio.criar_tabela_frases_recentes()
    
    def criar_tabela_associacao_frases_recentes_com_tabela_de_frases_agrupada_usando_Igualdade_como_criterio(self):
        print("criar_tabela_associacao_frases_recentes_com_tabela_de_frases_agrupada_usando_Igualdade_como_criterio")
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
        print("criar_tabela_associacao_frases_recentes_com_tabela_de_frases_agrupada_usando_linguagem_natural_como_criterio")
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
        print("criar_tabela_resultado_analise_textual")
        repositorio = PostagensRepository()
        associacoes_igualdade = repositorio.listarAssociacoes("Igualdade")
        associacoes_semantica = repositorio.listarAssociacoes("Equivalencia Semantica")
        
        resultado = dict()
        resultado["AUMENTO"]=0
        resultado["QUEDA"]=0
        
        for a in associacoes_igualdade:
            resultado[a["tendencia"]]+=1
        repositorio.insere_resultado_analise_textual(resultado, "igualdade")


        resultado = dict()
        resultado["AUMENTO"]=0
        resultado["QUEDA"]=0
        
        for a in associacoes_semantica:
            resultado[a["tendencia"]]+=1
        repositorio.insere_resultado_analise_textual(resultado, "Equivalencia Semantica")
    
    def carregar_dataset(self):
        print("carregar_dataset")
        sql = """select texto_tratado as texto, (case when aumento > queda then 1 else 0 end) as tendencia
                from frases f
                join mensagens m on m.mensagem=f.mensagem
                join frases_ocorrencias fo on md5(f.texto) = md5(fo.texto) and aumento<>queda 
                where texto_tratado is not null and aumento<>queda 
                order by m.data                """

        sql = """   with variacoes as (select cot2.valor/cot1.valor as variacao, cot1.data
                        from cotacoes cot1
                        join cotacoes cot2 on (cot1.data + interval '1 day')=cot2.data)
                    select string_agg(texto_tratado, ' ') as texto, 
                            (case when variacao>1.05 then 1 when variacao<0.95 then 0 else -1 end) as tendencia                    
                    from frases f
                    join mensagens m on m.mensagem=f.mensagem
                    join frases_ocorrencias fo on md5(f.texto) = md5(fo.texto)  and texto_tratado is not null
                    join variacoes v on v.data = date_trunc('hour', m.data) and (variacao<0.95 or variacao>1.05)
                    group by  v.data, v.variacao
                    order by v.data  """
        self.dataframe = sqlio.read_sql_query(sql, PostagensRepository().conexao())
        

        print ("carregado. Tamanho: {}".format(self.dataframe.size))

        import criptomante.util.machine_learn as machine_learn
        machine_learn.realizar_teste(self.dataframe)



    def gerar_frases(self, mensagem:Mensagem):        
        doc = self.nlp(mensagem.texto)
        frases = [self.strip(sent.string) for sent in doc.sents]
        frases = [f for f in frases if f!="" and not "http" in f]
        frases = list(OrderedDict.fromkeys(frases))
        return [Frase(f,mensagem.mensagem) for f in frases]

    def tratar_frases(self, frase:str):
        repositorio = PostagensRepository()        
        doc = self.nlp(str(frase))
        saida = dict()
        saida["original"] = frase
        saida["tratada"] = " ".join([f.lemma_.lower() for f in doc if not f.is_stop and f.is_alpha and not f.lemma_ in string.punctuation])
        repositorio.tratar_frases(saida)
        return ["ok"]
        
    
    def equivalencia_semantica(self, frase, frases_recentes):
        doc_frase_antiga = self.nlp(frase)
        frase_mais_proxima = None
        proximidade = 0
        for f in frases_recentes:
            doc_frase_recente = self.nlp(f)
            p = doc_frase_recente.similarity(doc_frase_antiga)
            if p>0.9 and p>proximidade:
                proximidade = p
                frase_mais_proxima = f
        return frase_mais_proxima
        
    def strip(self, frase:str):
        CARACTERES_A_SEREM_PERMITIDOS_NO_INICIO = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        CARACTERES_A_SEREM_PERMITIDOS_NO_FIM = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.!?,;"
        while (len(frase)>0) and (frase[0] not in CARACTERES_A_SEREM_PERMITIDOS_NO_INICIO):
            frase = frase[1:len(frase)]
        while (len(frase)>0) and (frase[len(frase)-1] not in CARACTERES_A_SEREM_PERMITIDOS_NO_FIM):
            frase = frase[0:len(frase)-1]
        return frase

    


    













if __name__ == "__main__":
    
    a = AnalyzerTextual()
    a.analisar()
    exit()









