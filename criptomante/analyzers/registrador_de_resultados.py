from datetime import datetime, timedelta
from criptomante.repository.resultados_repository import ResultadosRepository

class RegistradorDeResultados:
    def registrar_resultado_numerico(self):
        hoje = datetime.today()
        cotacoes = ResultadosRepository().cotacoes_medias_diarias(hoje - timedelta(days=180), hoje)
        previsoes = ResultadosRepository().listar_previsoes_numericas(hoje, 'media')
        registrar = cotacoes
        _data_anterior=None
        for _data in previsoes:
            if _data_anterior != None:
                diferenca = (_data["data"] - _data_anterior["data"]).days
                variacao = (_data["valor"] - _data_anterior["valor"])/diferenca
                for i in range(diferenca):
                    novo = dict()
                    novo["valor"] = _data_anterior["valor"] + (i*variacao)
                    novo["data"] = _data_anterior["data"] + timedelta(days=i)
                    novo["transacoes"] = 0
                    registrar.append(novo)
            _data_anterior = _data
        
        ResultadosRepository().gravar_previsao_numerica(registrar, 'media')

if __name__ == "__main__":
    RegistradorDeResultados().registrar_resultado_numerico()

