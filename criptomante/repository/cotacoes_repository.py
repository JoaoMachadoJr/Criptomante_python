from criptomante.repository.abstract_repository import AbstractRepository
from criptomante.model.plataforma import Plataforma
from typing import List
from criptomante.model.cotacao import Cotacao
from criptomante.model.snapshot import Snapshot

class CotacoesRepository(AbstractRepository):
    def listar_plataformas(self) -> List[Plataforma]:
        sql = "select nome, CAST(coalesce(ultimo,'01/01/1970') as timestamp without time zone) as ultimo from traders"
        result = self.fetchAll(sql, dict())
        saida = list()
        for p in result:
            plataforma = Plataforma()
            plataforma.nome = p["nome"]
            plataforma.ultimo = p["ultimo"]
            saida.append(plataforma)
        return saida
    
    def inserirCotacoes(self, cotacoes: List[Cotacao]):
        sql = """insert into cotacoes (data, valor, transacoes) values (:data, :valor, :transacoes)
                 ON CONFLICT (data) 
                 DO UPDATE SET valor = ((cotacoes.valor*cotacoes.transacoes)+(EXCLUDED.valor*EXCLUDED.transacoes))/(cotacoes.transacoes+EXCLUDED.transacoes), 
                 transacoes = cotacoes.transacoes+EXCLUDED.transacoes"""
        params = list()
        for c in cotacoes:
            p = dict()
            p["valor"] = c.valor
            p["transacoes"] = c.transacoes
            p["data"] = c.data
            params.append(p)
        self.executeMany(sql, params)

    def atualizarPlataforma(self, plataforma:str, ultimo):
        sql = "update traders set ultimo=:ultimo where nome=:nome"
        params = dict()
        params["nome"] = plataforma
        params["ultimo"] = ultimo
        self.execute(sql, params)


    def listarCotacoes(self):
        sql = "select data, valor, transacoes from cotacoes where data > '01/01/2014' order by data"
        result = self.fetchAll(sql, dict())
        saida = list()
        for r in result:
            c = Cotacao()
            c.data = r["data"]
            c.transacoes = r["transacoes"]
            c.valor = r["valor"]
            saida.append(c)
        return saida
    
    def gravarAnaliseNumerica(self, snapshot:Snapshot, algoritmo:str ):
        sql = "delete from temp.nprevisao where algoritmo = :algoritmo"
        self.execute(sql, {"algoritmo":algoritmo})
        
        sql = "insert into temp.nprevisao (data, label, valor, algoritmo) values (:data, :label, :valor, :algoritmo)"
        params = list()
        for key in snapshot.momentos:
            param=dict()
            param["data"] = snapshot.momentos[key].data
            param["label"] = snapshot.momentos[key].data.strftime("%d/%M/%y")
            param["valor"] = snapshot.momentos[key].valor
            param["algoritmo"] = algoritmo
            params.append(param)
        self.executeMany(sql, params)
        