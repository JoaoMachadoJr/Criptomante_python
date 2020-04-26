from criptomante.repository.abstract_repository import AbstractRepository
from datetime import datetime, timedelta
from criptomante.model.postagem import Postagem
from typing import List
from criptomante.model.mensagem import Mensagem
from criptomante.model.cotacao import Cotacao
from criptomante.model.snapshot import Snapshot
from criptomante.model.frase import Frase
import uuid
import random

class PostagensRepository(AbstractRepository):
    def obtemPostagensNaoProcessadas(self, limit=100, offset=0)->List[Postagem]:
        sql = "Select data, url, website from topicos where data_processamento is null order by data desc limit :limite offset :offset"
        result = self.fetchAll(sql, {"limite":limit, "offset":offset})
        saida = list()
        for obj in result:
            post = Postagem()
            post.data=obj["data"]
            post.website = obj["website"]
            post.url = obj["url"]
            saida.append(post)
        return saida

    def obtemPostagensNaoProcessadasComExecutor(self, limit=100, offset=0)->List[Postagem]:
        executor = random.randint(1,9999999)
        params = {"limite":limit, "offset":offset, "executor":executor}
        sql = """with aux as (select url from topicos where 
                    executor is null and 
                    (data_processamento is null or (data + ( interval '1 day'*2)) > cast(current_timestamp as date)) order by random() limit :limite offset :offset)
                update topicos as topicos
                set executor=:executor
                from aux aux
                where aux.url=topicos.url """
        self.execute(sql,params)

        sql = "Select data, url, website from topicos where  executor = :executor "
        result = self.fetchAll(sql,params)
        saida = list()
        for obj in result:
            post = Postagem()
            post.data=obj["data"]
            post.website = obj["website"]
            post.url = obj["url"]
            saida.append(post)
        return saida

    def limparExecutores(self):
        print("Limpando executores anteriores")
        sql = "update topicos set executor=null where executor is not null"
        self.execute(sql)
    
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

    def insereMensagens(self, mensagens: List[Mensagem], topico):
        sql = "select texto from mensagens where topico_url=:url"
        mensagens_ja_salvas = [d["texto"] for d in self.fetchAll(sql, {"url":topico})]

        
        sql = "insert into mensagens (texto, data, topico_url, mensagem) values (:texto, :data, :topico_url, :mensagem)"
        params = list()
        for mensagem in mensagens:
            if mensagem.texto in mensagens_ja_salvas:
                continue
            param = dict()
            param["texto"] = mensagem.texto
            param["data"] = mensagem.data
            param["topico_url"] = mensagem.postagem_url
            param["mensagem"] = uuid.uuid4()
            params.append(param)
        if len(params)==0:
            return
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

    def quantidade_frases_nao_tratadas(self):
        sql = "select count(*) as qnt from frases_ocorrencias where   texto_tratado is null"
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

    def listarFrasesNaoTratadas(self, limite:int = 1000000):
        sql = "select texto from frases_ocorrencias where  texto_tratado is null limit :limite"
        result = self.fetchAll(sql, {"limite": limite})
        saida = [r["texto"] for r in result]
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
        sql = "select count(*) as qnt from frases  f join mensagens m on m.mensagem=f.mensagem  where m.data<= :data_limite and not f.indexada"
        return self.fetchOne(sql, {"data_limite":data_limite_de_analise})["qnt"]
    
    def listarFrasesNaoIndexadas(self, limite:int, data_maxima:datetime):
        sql = "select f.texto, f.mensagem, f.frase, m.data from frases f join mensagens m on m.mensagem=f.mensagem where m.data<=:data_maxima and not f.indexada limit :limite"
        result = self.fetchAll(sql, {"data_maxima":data_maxima, "limite":limite})
        return [Frase(r["texto"], r["mensagem"], r["frase"], r["data"]) for r in result]
    
    def registrar_ocorrencia_frases_com_numero_aumentos_e_queda(self, parametros:List[dict]):
        sql = """insert into frases_ocorrencias  (texto, queda, aumento, estavel) values (:texto, :Queda, :Aumento, :Estavel)
                 ON CONFLICT (md5(texto)) 
                 DO UPDATE SET queda = frases_ocorrencias.queda + EXCLUDED.queda, 
                 aumento = frases_ocorrencias.aumento + EXCLUDED.aumento,
                 estavel = frases_ocorrencias.estavel + EXCLUDED.estavel"""
        self.executeMany(sql, parametros)
    
    def sinalizar_frases_indexadas(self, frases:List[str]):
        sql = "update frases set indexada=true where frase in :frases"
        params = {"frases":tuple(frases)}
        self.execute(sql, params)
    
    def criar_tabela_frases_recentes(self):
        sql = "select m.data from mensagens m join frases f using (mensagem) order by m.data desc limit 1"
        ultima_frase = self.fetchOne(sql)["data"]
        limite_inferior = ultima_frase - timedelta(hours=24)

        self.begin()
        sql = "delete from frases_recentes"
        self.execute(sql)
        sql = """
                insert into frases_recentes (frase, texto, mensagem)
                select frase, f.texto, f.mensagem
                from mensagens m
                join frases f on m.mensagem=f.mensagem
                where m.data>=:limite_inferior
        """
        self.execute(sql, {"limite_inferior":limite_inferior})
        self.commit()
    
    def listar_frases_recentes(self):
        sql = "select distinct texto from frases_recentes"
        result = self.fetchAll(sql)
        return [r["texto"] for r in result]
    
    def listar_frases_recentes_linguagem_natural(self):
        sql = "select distinct texto_tratado from frases_recentes f join frases_ocorrencias fo on md5(fo.texto) = md5(f.texto)"
        result = self.fetchAll(sql)
        return [r["texto_tratado"] for r in result]
    
    def listar_frases_com_tendencia(self):
        sql = """select texto from frases_ocorrencias fo
                where  (fo.aumento<>fo.queda)  and (aumento+queda)>3"""
        result = self.fetchAll(sql)
        return [r["texto"] for r in result]

    def listar_frases_com_tendencia_linguagem_natural(self):
        sql = """select texto_tratado from frases_ocorrencias fo
                where  (fo.aumento<>fo.queda)  and (aumento+queda)>3"""
        result = self.fetchAll(sql)
        return [r["texto_tratado"] for r in result]
    
    
    def insere_tabela_associacoes(self, associacoes, tipo_associacao):
        self.begin()
        sql = "delete from frases_associacoes where tipo_associacao=:tipo_associacao"
        self.execute(sql, {"tipo_associacao":tipo_associacao})
        sql = "insert into frases_associacoes (frase, frase_recente, tipo_associacao) values (:frase, :frase_recente, :tipo_associacao)"
        params = [{"frase":a["frase"], "frase_recente":a["frase_recente"], "tipo_associacao":tipo_associacao} for a in associacoes]
        self.executeMany(sql, params)
        self.commit()
    
    def preencher_tabela_associacoes_com_ocorrencias(self):
        sql = """
                with aux as (
                    select lower(fa.frase) as frase, sum(fo.queda) as queda, sum(fo.aumento) as aumento
                    from frases_associacoes fa
                    join frases_ocorrencias fo on  lower(fo.texto)=lower(fa.frase)
                    group by  lower(fa.frase)
                ) 
                update frases_associacoes as fa
                set tendencia = (case when fo.queda>fo.aumento then 'QUEDA' else 'AUMENTO' end),
                positividade = cast((cast(fo.aumento as numeric)/cast(fo.aumento + fo.queda as numeric)) as numeric)
                from aux fo
                where fo.frase=lower(fa.frase)
            """
        self.execute(sql)
    
    def tratar_frases(self, frases_tratadas):
        sql = "update frases_ocorrencias set texto_tratado=:tratada where md5(texto)=md5(:original)"
        self.execute(sql, frases_tratadas)

    def listarAssociacoes(self, tipo_associacao):
        sql = "select frase, frase_recente, tendencia from frases_associacoes where tipo_associacao = :tipo_associacao"
        return self.fetchAll(sql, {"tipo_associacao":tipo_associacao})
    
    def insere_resultado_analise_textual(self, parametros, tipo_associacao):
        sql = "delete from resultado_textual where tipo_associacao=:tipo_associacao"
        self.execute(sql, {"tipo_associacao":tipo_associacao})
        sql = "insert into resultado_textual (aumento, queda, tipo_associacao) values (:AUMENTO, :QUEDA, :tipo_associacao)"
        parametros["tipo_associacao"] = tipo_associacao
        self.execute(sql, parametros)
        self.commit()
    
    def remove_frases_vazias(self):
        print("Removendo frases vazias")
        sql = """
                delete from frases where REGEXP_REPLACE(texto, '[, 0123456789]+','') =''
                """        
        self.execute(sql)
        while (True):
            qnt = 0
            print("Frase vazia removida...")
            sql = "update frases set texto=SUBSTRING(texto from 2) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": ' %'}))

            sql = "update frases set texto=SUBSTRING(texto from 2) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": ',%'}))

            sql = "update frases set texto=SUBSTRING(texto from 2) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '1%'}))


            sql = "update frases set texto=SUBSTRING(texto from 2) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '2%'}))

            sql = "update frases set texto=SUBSTRING(texto from 2) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '3%'}))

            sql = "update frases set texto=SUBSTRING(texto from 2) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '4%'}))

            sql = "update frases set texto=SUBSTRING(texto from 2) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '5%'}))

            sql = "update frases set texto=SUBSTRING(texto from 2) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '6%'}))

            sql = "update frases set texto=SUBSTRING(texto from 2) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '7%'}))

            sql = "update frases set texto=SUBSTRING(texto from 2) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '8%'}))

            sql = "update frases set texto=SUBSTRING(texto from 2) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '9%'}))

            sql = "update frases set texto=SUBSTRING(texto from 2) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '0%'}))

            sql = "update frases set texto=SUBSTRING(texto from 1 for length(texto)-1) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '% '}))

            sql = "update frases set texto=SUBSTRING(texto from 1 for length(texto)-1) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '%,'}))

            sql = "update frases set texto=SUBSTRING(texto from 1 for length(texto)-1) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '%1'}))

            sql = "update frases set texto=SUBSTRING(texto from 1 for length(texto)-1) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '%2'}))

            sql = "update frases set texto=SUBSTRING(texto from 1 for length(texto)-1) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '%3'}))

            sql = "update frases set texto=SUBSTRING(texto from 1 for length(texto)-1) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '%4'}))

            sql = "update frases set texto=SUBSTRING(texto from 1 for length(texto)-1) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '%5'}))

            sql = "update frases set texto=SUBSTRING(texto from 1 for length(texto)-1) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '%6'}))

            sql = "update frases set texto=SUBSTRING(texto from 1 for length(texto)-1) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '%7'}))

            sql = "update frases set texto=SUBSTRING(texto from 1 for length(texto)-1) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '%8'}))

            sql = "update frases set texto=SUBSTRING(texto from 1 for length(texto)-1) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '%9'}))

            sql = "update frases set texto=SUBSTRING(texto from 1 for length(texto)-1) where texto like  :formato returning *"
            qnt += len(self.fetchAll(sql, {"formato": '%0'}))

            if qnt>0:
                print("Frases alteradas: {}".format(qnt))
                continue
            break                        
    
        sql = """
                delete from frases where REGEXP_REPLACE(texto, '[, 0123456789]+','') =''
                """        
        self.execute(sql)

    def texto_recente(self):
        #Pegar data de texto mais recente
        sql="select max(data) as data from mensagens"
        maior_data = self.fetchOne(sql)["data"]

        #Pegar texto
        sql_textos_recentes = """   
                                select string_agg(texto_tratado, '\n') as texto                    
                                from frases f
                                join mensagens m on m.mensagem=f.mensagem
                                join frases_ocorrencias fo on md5(f.texto) = md5(fo.texto)  and texto_tratado is not null
                                where m.data>(:data - interval '1 day')                                   
                              """ 
        return self.fetchOne(sql_textos_recentes, {"data":maior_data})["texto"]






    
        
        
        