from criptomante_crawler.crawlers.abstract_crawler import AbstractCrawler
from criptomante.util.json_util import JsonUtil
from bs4 import BeautifulSoup
from criptomante.model.postagem import Postagem
from criptomante.util.string_util import StringUtil
import datetime
from typing import List
from criptomante.model.mensagem import Mensagem
import time
import praw
from psaw import PushshiftAPI
from criptomante.util.log import Log

class CrawlerReddit(AbstractCrawler):
    def obtemPostagens(self, html:str)->List[Postagem]:
        conteudo = JsonUtil().decode(html)
        saida = list()
        for pagina in conteudo["data"]:
            postagem = Postagem()
            postagem.data = datetime.datetime.fromtimestamp(pagina["created_utc"]) #Converter de utc pra datetime
            postagem.data_processamento = None
            postagem.ultima_atualizacao = None
            postagem.url = pagina["full_link"]
            postagem.url = postagem.url.replace("www.reddit", "old.reddit")
            saida.append(postagem)
        return saida            
    
    def obtemMensagens(self, html:str, post:Postagem)->List[Mensagem]:
        saida = list()
        if "You must be 18+ to view this community" in html:
            return saida
        if "reddit removed this content" in html:
            return saida

        #Obtem titulo
        bs = BeautifulSoup(html, "lxml")
        result = bs.find_all("meta", {"property" : "og:title"})
        titulo = result[0].attrs["content"]
        mensagem_titulo = Mensagem()
        mensagem_titulo.data = post.data        
        mensagem_titulo.texto = titulo
        mensagem_titulo.postagem_url = post.url
        saida.append(mensagem_titulo)

        id_post = StringUtil.str_between(post.url, "comments/", "/")
        result = bs.find_all("input", {"value" : "t3_"+id_post})
        if (len(result)>0):
            result=result[0].parent
            result = result.find_all("div", {"class":"md"})
            if (len(result)>0):
                texto = result[0].text
                mensagem_texto_principal = Mensagem()
                mensagem_texto_principal.data = post.data
                mensagem_texto_principal.postagem_url = post.url
                mensagem_texto_principal.texto = texto
                saida.append(mensagem_texto_principal)

        #Obtem Comentários
        result = bs.find_all("div",{"data-type":"comment"} )
        for linha in result:
            result_data = linha.find_all("time", {"class": "live-timestamp"})
            if len(result_data)==0:
                data=post.data
            else:
                data = result_data[0].attrs["datetime"]
                data = datetime.datetime.strptime(data, "%Y-%m-%dT%H:%M:%S+00:00")    
                data.replace(tzinfo=None)
                data = data - datetime.timedelta(hours=3)  
            result_texto = linha.find_all("div", {"class":"md"})
            if len(result_texto)>0:                                          
                texto = result_texto[0].text
                comentario = Mensagem()
                comentario.data = data
                comentario.postagem_url = post.url
                comentario.texto = texto
                saida.append(comentario)
        return saida

    
    def obtemProximaPagina(self, html:str)->str:
        postagens = self.obtemPostagens(html)
        if len(postagens)==0:
            return None
        else:
            ultima_postagem = postagens[len(postagens)-1]
            datahora_utc = time.mktime(ultima_postagem.data.timetuple())
            subforum = StringUtil.str_between(self.url, "subreddit=","&")
            saida = "https://api.pushshift.io/reddit/search/submission/?subreddit=" +subforum+ "&sort_type=created_utc&sort=asc&after="+str(int(datahora_utc))
            print("URL: {}   A partir de: {}".format(saida, ultima_postagem.data.strftime("%d/%m/%Y %H:%M:%S")))
            return saida

if __name__ == "__main__":
    from criptomante_crawler.browser.browser import Browser
    
    post = Postagem()
    post.data = datetime.datetime.now()
    post.url = "https://old.reddit.com/r/Bitcoin/comments/eqgvc2/germany_additional_16_banks_with_negative/"
    html = Browser.lerHtmlComJs(post.url)
    crawler = CrawlerReddit()
    mensagens = crawler.obtemMensagens(html,post)

