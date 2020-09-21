from criptomante.repository.abstract_repository import AbstractRepository
from criptomante.model.snapshot import Snapshot, Momento
from criptomante.model.cotacao import Cotacao
from typing import List
import datetime

class ResultadosRepository(AbstractRepository):
    def cotacoes_medias_diarias(self, limite_inferior, limite_superior):
        sql = """select date_trunc('day', data) as data, avg(valor) valor, sum(transacoes) as transacoes
                from cotacoes c
                where c.data between :limite_inferior and :limite_superior
                group by date_trunc('day', data)
                order by date_trunc('day', data)"""
        return self.fetchAll(sql, {"limite_inferior":limite_inferior, "limite_superior":limite_superior})

    def listar_previsoes_numericas(self, limite_inferior, algoritmo):
        sql = """select data, valor
                from temp.nprevisao p
                where p.data>=:limite_inferior and p.algoritmo=:algoritmo
                order by data
                """
        return self.fetchAll(sql, {"limite_inferior":limite_inferior, "algoritmo":algoritmo})

    
    def gravar_previsao_numerica(self, dados, algoritmo):
        self.begin()
        sql = "delete from graficos.previsao_numerica"
        self.execute(sql)
        sql = "insert into graficos.previsao_numerica (data, valor, transacoes, algoritmo) values (:data, :valor, :transacoes, :algoritmo)"
        self.executeMany(sql, [{"data": p["data"], "valor": p["valor"], "transacoes": p["transacoes"], "algoritmo": algoritmo} for p in dados])
        self.commit()

    def listar_previsao_numerica(self, algoritmo):        
        sql = "select * from graficos.previsao_numerica where algoritmo=:algoritmo order by data"
        return self.fetchAll(sql, {"algoritmo": algoritmo})

    def listar_comentarios_recentes(self, prefixo = ''):        
        sql = """select frase, tipo, url_topico, aumentos, quedas from graficos.comentarios_recentes  where tipo like (:prefixo||'%%') and CHAR_LENGTH(frase)>10                 
                order by GREATEST(cast(aumentos/cast(aumentos+quedas as numeric) as numeric),cast(quedas/cast(quedas+aumentos as numeric) as numeric)) desc"""
        return self.fetchAll(sql, {"prefixo":prefixo})


    def inserir_snapshots(self, snapshots: List[Snapshot]):
        self.begin()
        sql = "delete from graficos.snapshots"        
        self.execute(sql)
        sql = """
            insert into graficos.snapshots (dr, sigla, data, variacao, valor, tipo, pontuacao, peso, data_referencia)
            values (:dr, :sigla, :data, :variacao, :valor, :tipo, :pontuacao, :peso, :data_referencia)
        """
        batch = []
        for s in snapshots:            
            for momento in s.momentos.keys():
                param= dict()
                param["dr"] = s.data
                param["sigla"] = momento
                param["data"] = s.momentos[momento].data
                param["variacao"] = s.momentos[momento].variacao
                param["valor"] = s.momentos[momento].valor
                if s.momentos[momento].data < s.data:
                    param["tipo"] = "Passado"
                elif s.momentos[momento].data > s.data:
                    param["tipo"] = "Futuro"
                else:
                    param["tipo"] = "Presente"
                param["pontuacao"] = s.pontuacao
                param["peso"] = pow(s.pontuacao, s.pontuacao)
                param["data_referencia"] = s.momentos[momento].data_referencia
                batch.append(param)
        self.executeMany(sql, batch)
        self.commit()

    def listar_snapshots(self):
        sql = "select * from graficos.snapshots order by dr, data"
        result = self.fetchAll(sql)

        return self.resultset_to_snapshots(result)
        
    def listar_snapshot_mais_recente(self):
        sql = """select * 
                    from graficos.snapshots s 
                    where s.dr = (select max(s2.dr) from graficos.snapshots s2) 
                        and s.tipo <> 'Futuro'
                    order by dr, data desc"""
        result = self.fetchAll(sql)
        return self.resultset_to_snapshots(result)
    
    def listar_snapshot_maior_pontuacao(self):
        sql = """select * 
                from graficos.snapshots s 
                where s.pontuacao in (select distinct s2.pontuacao from graficos.snapshots s2 order by s2.pontuacao desc limit 3) 
                    and s.dr <> (select max(s3.dr) from graficos.snapshots s3)
                order by s.pontuacao desc, dr, data
        """
        result = self.fetchAll(sql)
        return self.resultset_to_snapshots(result)

    def resultset_to_snapshots(self, resultset):
        saida = list()
        ultimo_dr = datetime.date(2000,1,1)
        s :Snapshot= None
        for r in resultset:
            if r["dr"]!=ultimo_dr:
                s = Snapshot()
                s.data = r["dr"]
                s.pontuacao = r["pontuacao"]
                ultimo_dr = r["dr"]
                saida.append(s)
            s.momentos[r["sigla"]] = Momento.encode(r["data"], r["data_referencia"])
            s.momentos[r["sigla"]].valor = r["valor"]
            s.momentos[r["sigla"]].variacao = r["variacao"]
        return saida


                
                

    def gravar_dados_tabela_comentarios(self):
        self.begin()
        sql = "delete from graficos.comentarios_recentes"
        self.execute(sql)
        sql = """insert into graficos.comentarios_recentes (frase, tipo, url_topico, aumentos, quedas)
                select fr.texto, 
                        (case when sum(fo.aumento)>sum(fo.queda) then 'Frase Positiva' else 'Frase Negativa' end),
                        min(m.topico_url),
                        sum(fo.aumento),
                        sum(fo.queda)
                from frases_recentes fr
                join mensagens m on m.mensagem=fr.mensagem
                join frases_ocorrencias fo on lower(fo.texto)=lower(fr.texto)
                join frases_associacoes fa on fa.frase_recente=fr.texto
                where char_length(fr.texto)>=4
                group by 1
                having(sum(fo.aumento)<>sum(fo.queda))
                """
        self.execute(sql)
        self.commit()

    
    def gravar_analise_semantica(self, dados):
        sql =   """
                    insert into graficos.analise_semantica
                        (tp,
                        fp,
                        tn,
                        fn,
                        accuracy,
                        "precision",
                        recall,
                        previsao)
                    values
                        (:tp,
                        :fp,
                        :tn,
                        :fn,
                        :accuracy,
                        :precision,
                        :recall,
                        :previsao)
                """

        params=dict()
        params["tp"] = dados["TP"]
        params["fp"] = dados["FP"]
        params["tn"] = dados["TN"]
        params["fn"] = dados["FN"]
        params["accuracy"] = dados["Accuracy"]
        params["precision"] = dados["Precision"]
        params["recall"] = dados["Recall"]
        params["previsao"] = None

        self.execute(sql,params)

    def recuperar_analise_semantica(self):
        sql = """
            select *
            from graficos.analise_semantica
            order by datahora desc
            limit 1
            """
        return self.fetchOne(sql)

    def gravar_previsao_analise_semantica(self, resultado):
        sql="select max(datahora) as datahora from graficos.analise_semantica"
        maior_data = self.fetchOne(sql)["datahora"]
        
        sql="update graficos.analise_semantica set previsao=:previsao where datahora=:datahora"
        self.execute(sql, {"datahora":maior_data,"previsao":resultado })

        
"""select m.data::date,
		count(m.mensagem),
		avg(c1.valor),
		avg(c2.valor)
from mensagens m
join cotacoes c1 on c1.data::date = m.data::date
join cotacoes c2 on c2.data::date = (m.data::date + interval '1 day')
where m.data::date between '01/01/2020' and '31/01/2020'
group by m.data::date"""