from criptomante.repository.abstract_repository import AbstractRepository
from datetime import datetime, timedelta
from criptomante.model.postagem import Postagem
from typing import List
from criptomante.model.mensagem import Mensagem
from criptomante.model.cotacao import Cotacao
from criptomante.model.snapshot import Snapshot
from criptomante.model.frase import Frase
import uuid

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
        sql = "insert into topicos (website, url, data) values (:website, :url, :data) on conflict (url) do nothing"
        params = dict()
        params["website"] = postagem.website
        params["url"] = postagem.url
        params["data"] = postagem.data
        self.execute(sql, params)
    
    def inserePostagens(self, postagens:List[Postagem]):
        sql = "insert into topicos (website, url, data) values (:website, :url, :data) on conflict (url) do nothing"
        params = [{"website":postagem.website, "url":postagem.url, "data":postagem.data} for postagem in postagens]
        self.executeMany(sql, params)

    def insereMensagens(self, mensagens: List[Mensagem]):
        sql = "insert into mensagens (texto, data, topico_url, mensagem) values (:texto, :data, :topico_url, :mensagem)"
        params = list()
        if len(mensagens)==0:
            return
        for mensagem in mensagens:
            param = dict()
            param["texto"] = mensagem.texto
            param["data"] = mensagem.data
            param["topico_url"] = mensagem.postagem_url
            param["mensagem"] = uuid.uuid4()
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
        result = self.fetchAll(sql)
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
        data_minima = self.fetchOne(sql)["data_minima"]

        sql = "select texto, data, mensagem from mensagens where data>= :data_minima"
        result = self.fetchAll(sql, {"data_minima": data_minima})
        saida = list()
        for r in result:
            m = Mensagem()
            m.mensagem = r["mensagem"]
            m.data = r["data"]
            m.texto = r["texto"]
            saida.append(m)
        return saida
    
    def quantidade_mensagens_nao_indexadas(self):
        sql = "select count(*) as qnt from mensagens where not indexada"
        return self.fetchOne(sql)["qnt"]
    
    def listarMensagensNaoIndexadas(self, limite:int = 1000000):
        sql = "select mensagem, texto, data, topico_url from mensagens where not indexada limit :limite"
        result = self.fetchAll(sql, {"limite": limite})
        saida = list()
        for r in result:
            m = Mensagem()
            m.mensagem = r["mensagem"]
            m.data = r["data"]
            m.texto = r["texto"]
            m.postagem_url = r["topico_url"]
            saida.append(m)
        return saida

    def inserir_tabela_frases(self, frases : List[Frase]):        
        sql = "insert into frases (texto, mensagem, frase) values (:texto, :mensagem, :frase)"
        params = [ {"texto":f.texto, "mensagem":f.mensagem, "frase": uuid.uuid4()} for f in frases]
        self.executeMany(sql, params)
        
    def sinalizar_mensagens_indexadas(self, mensagens:List[str]):
        sql = "update mensagens set indexada=true where mensagem in :mensagens"
        params = {"mensagens":tuple(mensagens)}
        self.execute(sql, params)

    def quantidade_frases_nao_indexadas(self, data_limite_de_analise):
        sql = "select count(*) as qnt from frases where data<= :data_limite and not indexada"
        return self.fetchOne(sql, {"data_limite":data_limite_de_analise})["qnt"]
    
    def listarFrasesNaoIndexadas(self, limite:int, data_maxima:datetime):
        sql = "select texto, mensagem, frase from frases f join mensagens m on m.mensagem=f.mensagem where m.data<=:data_maxima and not f.indexada limit :limite"
        result = self.fetchAll(sql, {"data_maxima":data_maxima, "limite":limite})
        return [Frase(r["texto"], r["mensagem"], r["frase"]) for r in result]
    
    def registrar_ocorrencia_frases_com_numero_aumentos_e_queda(self, parametros:List[dict]):
        sql = """insert into frases_ocorrencias f (texto, queda, aumento, estavel) values (:texto, :queda, :aumento, :estavel)
                 ON CONFLICT (texto) 
                 DO UPDATE SET queda = f.queda + EXCLUDED.queda, 
                 aumento = f.aumento + EXCLUDED.aumento,
                 estavel = f.estavel + EXCLUDED.estavel"""
        self.executeMany(sql, parametros)
    
    def sinalizar_frases_indexadas(self, frases:List[str]):
        sql = "update frases set indexada=true where frase in :frases"
        params = {"frases":tuple(frases)}
        self.execute(sql, params)
    
    def criar_tabela_frases_recentes(self):
        sql = "select max(m.data) as data from frases f join mensagens m on m.mensagem=f.mensagem"
        ultima_frase = self.fetchOne(sql)["data"]
        limite_inferior = ultima_frase - timedelta(hours=24)

        self.begin()
        sql = "delete from frases_recentes"
        self.execute(sql)
        sql = """
                insert into frases_recentes (frase, texto, mensagem)
                select frase, texto, mensagem
                from frases f
                join mensagens m on m.mensagem=f.mensagem
                where m.data>=:limite_inferior
        """
        self.execute(sql, {"limite_inferior":limite_inferior})
        self.commit()
    
    def listar_frases_recentes(self):
        sql = "select distinct texto from frases_recentes"
        result = self.fetchAll(sql)
        return [r["texto"] for r in result]
    
    def listar_frases_com_tendencia(self):
        sql = """select texto from frases_ocorrencias fo
                where  (fo.aumento>3 or fo.queda>3)
                and ((fo.aumento>fo.queda*3) or (fo.queda>fo.aumento*3))"""
        result = self.fetchAll(sql)
        return [r["texto"] for r in result]
    
    
    def insere_tabela_associacoes(self, associacoes, tipo_associacao):
        self.begin()
        sql = "delete from frases_associacoes where tipo_associacao=:tipo_associacao"
        self.execute(sql)
        sql = "insert into frases_associacoes (frase, frase_recente, tipo_associacao) values (:frase, :frase_recente, :tipo_associacao)"
        params = [{"frase":a["frase"], "frase_recente":a["frase_recente"], "tipo_associacao":tipo_associacao} for a in associacoes]
        self.executeMany(sql, params)
        self.commit()
    
    def preencher_tabela_associacoes_com_ocorrencias(self):
        sql = """ 
                update frases_associacoes as fa
                set tendencia = (case when fo.queda>fo.aumento then 'QUEDA' else 'AUMENTO' end)
                where fo.frase=fa.texto
            """
        self.execute(sql)

    def listarAssociacoes(self, tipo_associacao):
        sql = "select frase, frase_recente, queda, aumento, estavel from frases_associacoes where tipo_associacao = :tipo_associacao"
        return self.fetchAll(sql, {"tipo_associacao":tipo_associacao})
    
    def insere_resultado_analise_textual(self, parametros, tipo_associacao):
        sql = "delete from resultado_textual where tipo_associacao=:tipo_associacao"
        self.execute(sql, {"tipo_associacao":tipo_associacao})
        sql = "insert into resultado_textual (aumento, queda, tipo_associacao) values (:aumento, :queda, :tipo_associacao)"
        parametros["tipo_associacao"] = tipo_associacao
        self.execute(sql, parametros)
        self.commit()






    
        
        
        