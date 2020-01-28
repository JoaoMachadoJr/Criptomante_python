from threading import Thread
from typing import List
from criptomante.repository.postagensRepository import PostagensRepository

from criptomante.util.thread_util import ThreadUtil
from criptomante.model.postagem import Postagem
from criptomante_crawler.threads.minha_thread import MinhaThread

class ThreadPostagens(MinhaThread):
    #Atributos estáticos
    name = "ThreadPostagens"
    

    #Atributos de instância
    url : str
    crawler: object
    post: Postagem

    @classmethod
    def getCrawler(cls, website)->"AbstracrCrawler":
        from criptomante_crawler.crawlers.crawler_reddit import CrawlerReddit
        return CrawlerReddit()
    
    def executar(self):
        
        mensagens = self.crawler.processar_postagem(self.url, self.post)
        #print("Sucesso ao ler MSG "+self.url)                
        repository = PostagensRepository()
        repository.insereMensagens(mensagens)
        repository.sinalizaPostagemProcessada(self.url)
        self.continuar=False
        #print("Sucesso ao gravar MSG "+self.url)
                                                
    def mensagem_erro(self, e):
        print("Erro ao ler MSG "+self.url)
        print(e)
    
    @classmethod
    def fabricar(cls, limite,offset=0):
        saida=list()
        from criptomante_crawler.crawlers.abstract_crawler import AbstractCrawler
        repository = PostagensRepository()
        postagens = repository.obtemPostagensNaoProcessadas(limit=limite)
        if len(postagens)==0:
            ThreadPostagens.esperando_novos = False
        for postagem in postagens:
            crawler:AbstractCrawler = ThreadPostagens.getCrawler(postagem.website)
            t = ThreadPostagens()
            t.url = postagem.url
            t.crawler = crawler
            t.post = postagem            
            saida.append(t)
        return saida

class GerenciadorThreadPostagens(Thread):
    esperando_novos:bool = False

    def run(self):
        self.name = "GerenciadorThreadPostagens"
        self.gerenciar()
    @classmethod
    def gerenciar(cls):
        from criptomante_crawler.crawlers.abstract_crawler import AbstractCrawler
        from time import sleep
        repository = PostagensRepository()
        while (True):
            fila_threads = list()
            postagens = repository.obtemPostagensNaoProcessadas()
            if len(postagens)==0:
                if (not GerenciadorThreadPostagens.esperando_novos):
                    return
                sleep(10)
            else:
                for postagem in postagens:
                    crawler:AbstractCrawler = ThreadPostagens.getCrawler(postagem.website)
                    t = ThreadPostagens()
                    t.url = postagem.url
                    t.crawler = crawler
                    t.post = postagem
                    fila_threads.append(t)
                    t.start()
                ThreadUtil.esperar(fila_threads, "de Postagens")
            
            
            
if __name__ == "__main__":
    GerenciadorThreadPostagens().gerenciar()    
                

        





    