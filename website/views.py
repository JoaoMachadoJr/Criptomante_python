from django.shortcuts import render
from criptomante.repository.nprevisoes import NPrevisoesRepository
from criptomante.repository.resultados_repository import ResultadosRepository
# Create your views here.
def website(request):
    """repository = NPrevisoesRepository()
    listagem = repository.listar(60)
    params = dict()
    params["listagem"] = listagem
    params["datas"] = repository.datas(listagem)
    params["labels"] = repository.labels(listagem)
    params["valores"] = repository.valores(listagem)
    params["limite_inferior"] = repository.limite_inferior(listagem)"""
    params = dict()
    previsao_numerica = ResultadosRepository().listar_previsao_numerica('media')
    params["previsao_numerica"] = [ [p["data"].strftime("%Y-%m-%d"), float(p["valor"])] for p in previsao_numerica]
    params["transacoes"] = [ [p["data"].strftime("%Y-%m-%d"), float(p["transacoes"])] for p in previsao_numerica]
    return render(request, 'website.html', params)