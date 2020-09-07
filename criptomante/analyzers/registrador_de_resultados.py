from datetime import datetime, timedelta
from criptomante.repository.resultados_repository import ResultadosRepository
from dateutil import relativedelta

class RegistradorDeResultados:
    def registrar_resultados(self):
        self.registrar_resultado_numerico()
        self.registrar_resultado_textual()
    
    def registrar_resultado_textual(self):
        print("registrar_resultado_textual")
        ResultadosRepository().gravar_dados_tabela_comentarios()
        
    def registrar_resultado_numerico(self):
        print("registrar_resultado_numerico")
        hoje = datetime.today()
        cotacoes = ResultadosRepository().cotacoes_medias_diarias(hoje - relativedelta.relativedelta(months=12), hoje)
        previsoes = ResultadosRepository().listar_previsoes_numericas(hoje, 'mediana')
        registrar = cotacoes
        _data_anterior=None
        for _data in previsoes:
            if (_data_anterior != None) and (_data["data"]<hoje+timedelta(days=100)):
                diferenca = (_data["data"] - _data_anterior["data"]).days
                variacao = (_data["valor"] - _data_anterior["valor"])/diferenca
                for i in range(diferenca):
                    novo = dict()
                    novo["valor"] = _data_anterior["valor"] + (i*variacao)
                    novo["data"] = _data_anterior["data"] + timedelta(days=i)
                    novo["transacoes"] = 0
                    registrar.append(novo)
            _data_anterior = _data
        
        ResultadosRepository().gravar_previsao_numerica(registrar, 'mediana')

        

if __name__ == "__main__":
    RegistradorDeResultados().registrar_resultados()

