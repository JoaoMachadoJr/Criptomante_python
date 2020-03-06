from django.shortcuts import render
from criptomante.repository.nprevisoes import NPrevisoesRepository
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
    return render(request, 'website.html', dict())