from criptomante.repository.abstract_repository import AbstractRepository
from datetime import datetime
from criptomante.model.postagem import Postagem
from typing import List
from criptomante.model.mensagem import Mensagem
from criptomante.model.cotacao import Cotacao
from criptomante.model.snapshot import Snapshot

class PostagensRepository(AbstractRepository):
    def obtemPostagensNaoProcessadas(self, limit=100, offset=0)->List[Postagem]:
        sql = "Select data, url, website from topicos where data_processamento is null order by data  limit :limite offset :offset"
        result = self.fetchAll(sql, {"limite":limit, "offset":offset})
        saida = list()
        for obj in result:
            post = Postagem()
            post.data=obj["data"]
            post.website = obj["website"]
            post.url = obj["url"]
            saida.append(post)
        return saida
    
    def obtemPostagemMaisRecente(self, offset = 0)->Postagem:
        sql = "select data, url, website from topicos order by data desc offset :offset limit 1"
        result = self.fetchOne(sql, {"offset":offset})
        if result != None:
            post = Postagem()
            post.data = result["data"]
            post.website = result["website"]
            post.url = result["url"]
            return post
        else:
            return None
    
    def inserePostagem(self, postagem:Postagem):
        params = dict()
        params["website"] = postagem.website
        params["url"] = postagem.url
        params["data"] = postagem.data

        sql = "Select * from topicos where url=:url"
        if self.fetchOne(sql, params) == None:
            sql = "insert into topicos (website, url, data) values (:website, :url, :data)"
            params = dict()
            params["website"] = postagem.website
            params["url"] = postagem.url
            params["data"] = postagem.data
            self.execute(sql, params)

    def insereMensagens(self, mensagens: List[Mensagem]):
        sql = "insert into mensagens (texto, data, topico_url) values (:texto, :data, :topico_url)"
        params = list()
        if len(mensagens)==0:
            return
        for mensagem in mensagens:
            param = dict()
            param["texto"] = mensagem.texto
            param["data"] = mensagem.data
            param["topico_url"] = mensagem.postagem_url
            params.append(param)
        self.executeMany(sql, params)
    
    def sinalizaPostagemProcessada(self, url):
        sql = "update topicos set data_processamento=:agora where url=:url"
        params = dict()
        params["agora"] = datetime.now()
        params["url"] = url
        self.execute(sql, params)

    def listarMensagens(self):
        sql = "select texto, data, mensagem from mensagens"
        result = self.fetchAll(sql, dict())
        saida = list()
        for r in result:
            m = Mensagem()
            m.mensagem = r["mensagem"]
            m.data = r["data"]
            m.texto = r["texto"]
            saida.append(m)
        return saida

    def listarMensagensRecentes(self):
        sql = "select (max(data) - interval '1 day') as data_minima from mensagens"
        data_minima = self.fetchOne(sql,dict())["data_minima"]

        sql = "select texto, data, mensagem from mensagens where data>= :data_minima"
        result = self.fetchAll(sql, {"data_minima", data_minima})
        saida = list()
        for r in result:
            m = Mensagem()
            m.mensagem = r["mensagem"]
            m.data = r["data"]
            m.texto = r["texto"]
            saida.append(m)
        return saida
    
        
        
        