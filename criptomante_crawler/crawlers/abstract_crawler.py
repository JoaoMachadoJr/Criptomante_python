if __name__ == "__main__":
    from dotenv import load_dotenv
    from pathlib import Path
    env_path = Path('C:\\@work\\pessoal\\Criptomante_python\\.env')
    load_dotenv(dotenv_path=env_path)

    import sys
    sys.path.append('C:\\@work\\pessoal\\Criptomante_python')


from abc import ABC, abstractmethod
from criptomante.util.log import Log
from criptomante_crawler.browser.browser import Browser
from criptomante_crawler.threads.thread_postagens import ThreadPostagens
from criptomante.repository.postagensRepository import PostagensRepository
from criptomante.model.postagem import Postagem
from typing import List
from criptomante.model.mensagem import Mensagem
import time
from criptomante_crawler.threads.gerenciador_threads import GerenciadorThreads
from time import sleep

class AbstractCrawler(ABC):
    def __init__(self, url:str = None):
        self.url=url
        self.website = None
    
    def navegar(self, iniciar_leitor_postagens :bool= True)->None:
        Log.info("Iniciou operação: "+self.url)
        repositorio = PostagensRepository()

        if iniciar_leitor_postagens:
            GerenciadorThreads.iniciar_gerenciamento(ThreadPostagens, True, quantidade_limite=10, fabricar=True, timeout=120)
                
        while (self.url!=None):
            try:                
                postagens = self.processar_menu(self.url)
                for postagem in postagens:
                    postagem.website = self.website
                repositorio.inserePostagens(postagens)
                proxima = self.obtemProximaPagina(self.html)
                self.url=proxima
                    
            except Exception as e:
                print(str(e))
        sleep(10)
        print("Proxima url nula, sinalizando para parar execucao")    
        if iniciar_leitor_postagens:
            print("crawler recebeu pedido para parar")
            GerenciadorThreads.finalizar_gerenciamento(ThreadPostagens, continuar_esperando_novas_threads=True)
            print("crawler parou")


    def processar_menu(self, url_menu: str)->List[Postagem]:
        html = Browser.lerHtml(url_menu)
        self.html = html
        postagens = self.obtemPostagens(html)
        return postagens
                
    def processar_postagem(self, url_postagem : str, post:Postagem)->List[Mensagem]:
        html = Browser.lerHtml(url_postagem)
        mensagens = self.obtemMensagens(html, post)
        return mensagens

    @abstractmethod
    def obtemMensagens(self, html:str, post:Postagem) -> List[Mensagem]:
        #TODO
        return list()

    @abstractmethod
    def obtemPostagens(self, html:str) ->List[Postagem]:
        #TODO
        return list()

    @abstractmethod
    def obtemProximaPagina(self, html:str) ->str:
        #TODO
        return ""

if __name__ == "__main__":
    from criptomante_crawler.crawlers.crawler_reddit import CrawlerReddit
    repositorio = PostagensRepository()
    ultima = repositorio.obtemPostagemMaisRecente(100)
    if ultima==None:
        crawler = CrawlerReddit("https://api.pushshift.io/reddit/search/submission/?subreddit=Bitcoin&sort_type=created_utc&sort=asc")
    else:
        crawler = CrawlerReddit("https://api.pushshift.io/reddit/search/submission/?subreddit=Bitcoin&sort_type=created_utc&sort=asc&after={}".format(int(time.mktime(ultima.data.timetuple()))))
    crawler.website = "reddit.com/r/BitcoinMarkets"
    crawler.navegar()

    
#12->41
#25 -> 80
#50 ->80


