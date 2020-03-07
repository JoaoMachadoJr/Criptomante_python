from criptomante.repository.abstract_repository import AbstractRepository

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
        
