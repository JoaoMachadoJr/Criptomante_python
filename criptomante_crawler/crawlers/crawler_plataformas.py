from criptomante.repository.cotacoes_repository import CotacoesRepository
from datetime import timedelta, datetime
from threading import Thread
from criptomante_crawler.browser.browser import Browser
from criptomante.model.plataforma import Plataforma
import gzip
import shutil
from csv import DictReader
from criptomante.model.cotacao import Cotacao
import time
import subprocess
import os 
import mmap

class CrawlerPlataformas:
    LIMITE_PARA_USO_DA_ROTINA_PARA_COTACOES_RECENTES = timedelta(days=4)
        
    def consultar_dados_antigos(self):
        URL_API = "http://api.bitcoincharts.com/v1/csv/{}.csv.gz".format(self.plataforma.nome)
        CAMINHO_ARQUIVO = "/temp/{}.csv".format(self.plataforma.nome)
        CAMINHO_ARQUIVO_COMPACTADO = CAMINHO_ARQUIVO+".gz"        

        #Download do arquivo CSV
        #Browser.download(URL_API,CAMINHO_ARQUIVO_COMPACTADO)
        #self.descompactar_arquivo(CAMINHO_ARQUIVO_COMPACTADO, CAMINHO_ARQUIVO)

        #Ler arquivo CSV
        cotacoes = set()
        linhas = self.contar_linhas(CAMINHO_ARQUIVO)
        print("Arquivo de {} linhas".format(linhas))
        i=0
        with open(CAMINHO_ARQUIVO, newline='') as csvfile:
            reader = DictReader(csvfile, ["timestamp", "valor"])
            for row in reader:
                cotacao = Cotacao()
                cotacao.data = datetime.fromtimestamp(int(row["timestamp"]))
                cotacao.data = cotacao.data.replace(tzinfo=None)
                cotacao.valor = float(row["valor"])
                cotacao.transacoes=1                
                if self.plataforma.ultimo<cotacao.data:
                    cotacoes.add(cotacao)
                    self.plataforma.ultimo = cotacao.data
                if len(cotacoes)==100000:
                    self.gravar(cotacoes)
                    cotacoes.clear()
                    print("Cotacoes salvas: {}/{}".format(i,linhas))
                i=i+1
                
        
        #Gravar
        self.gravar(cotacoes)

    def contar_linhas(self, nome_arquivo):
        CAMINHO_ABSOLUTO = os.path.abspath(nome_arquivo)
        with open(nome_arquivo) as f:
            for line_number, _ in enumerate(f, 1):
                pass         
        return line_number

    def gravar(self, cotacoes):
        cotacoes = Cotacao.agrupar(cotacoes)
        repositorio = CotacoesRepository()
        repositorio.inserirCotacoes(cotacoes)
        repositorio.atualizarPlataforma(self.plataforma.nome, self.plataforma.ultimo)

        
        
                
                
    
    def consultar_dados_recentes(self):
        URL = "http://api.bitcoincharts.com/v1/trades.csv?symbol={}&start={}"
        URL = URL.format(self.plataforma.nome, int(self.plataforma.ultimo.timestamp()))

        csv = Browser.lerHtml(URL)

        #Ler CSV
        reader = DictReader(csv.splitlines(), ["timestamp", "valor"])
        cotacoes = set()
        i=0
        for row in reader:
            cotacao = Cotacao()
            cotacao.data = datetime.fromtimestamp(int(row["timestamp"]))
            cotacao.data = cotacao.data.replace(tzinfo=None)
            cotacao.valor = float(row["valor"])
            cotacao.transacoes=1                
            if self.plataforma.ultimo<cotacao.data:
                cotacoes.add(cotacao)
                self.plataforma.ultimo = cotacao.data
            if len(cotacoes)==100000:
                self.gravar(cotacoes)
                cotacoes.clear()
                print("Cotacoes salvas: {}".format(i))
            i=i+1
                
        
        #Gravar
        self.gravar(cotacoes)


    def consultar_todas_as_plataformas(self, paralelo = False):
        repository = CotacoesRepository()
        plataformas = repository.listar_plataformas()        
        
        lista_threads = list() #Usado apenas para execução paralela
        for p in plataformas:
            if (p.ultimo + self.LIMITE_PARA_USO_DA_ROTINA_PARA_COTACOES_RECENTES)<datetime.now():
                if paralelo:
                    crawler = CrawlerPlataformas()
                    crawler.plataforma = p
                    t = Thread(target=crawler.consultar_dados_antigos)
                    t.name = "Consultar_dados_antigos"
                    lista_threads.append(t)
                    t.start()
                else:
                    self.plataforma = p
                    self.consultar_dados_antigos()
            else:
                if paralelo:
                    crawler = CrawlerPlataformas()
                    crawler.plataforma = p
                    t = Thread(target=crawler.consultar_dados_recentes)
                    t.name = "Consultar_dados_recentes"
                    lista_threads.append(t)
                    t.start()
                else:
                    self.plataforma = p
                    self.consultar_dados_recentes()
    
    def descompactar_arquivo(self, arquivo_origem, arquivo_destino):
        with gzip.open(arquivo_origem, 'rb') as f_in:
            with open(arquivo_destino, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

if __name__ == "__main__":
    crawler = CrawlerPlataformas()
    crawler.consultar_todas_as_plataformas()


                
