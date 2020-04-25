from django.shortcuts import render
from criptomante.repository.nprevisoes import NPrevisoesRepository
from criptomante.repository.resultados_repository import ResultadosRepository


# Create your views here.
def dashboard(request):
    from django.templatetags.static import static
    
    params = dict()
    previsao_numerica = ResultadosRepository().listar_previsao_numerica('media')
    params["previsao_numerica"] = [ [p["data"].strftime("%Y-%m-%d"), float(p["valor"])] for p in previsao_numerica]
    params["transacoes"] = [ [p["data"].strftime("%Y-%m-%d"), float(p["transacoes"])] for p in previsao_numerica]
    params["qnt_frases_tendencia"]=5
    params["comentarios_recentes"] = list()
    comentarios_recentes = ResultadosRepository().listar_comentarios_recentes("Frase")
    for c in comentarios_recentes[1:100]:
        elem = dict()
        elem["Comentário"] = '<td style="width: 70%" ><div class="d-flex align-items-center"><div><div class="text-default font-size-md"> '+c["frase"]+"</div></div> </div></td>"
        if c["aumentos"]>c["quedas"]:
            elem["Tendência"] =  '<td><span class="text-success-600"><i class="icon-stats-growth2 mr-2"></i> '+"{:3.2f}".format(((c["aumentos"]/ (c["aumentos"]+c["quedas"]))*100))+'%</span></td>'
        else:
            elem["Tendência"] =  '<td><span class="text-danger"><i class="icon-stats-decline2 mr-2"></i> '+"{:3.2f}".format(((c["quedas"]/ (c["aumentos"]+c["quedas"]))*100))+'%</span></td>'
        elem["Ver publicação"] =  '<td><div class="d-flex justify-content-center"><a href="'+c["url_topico"]+'"><img src="{url}"'.format(url=static('images/reddit.png'))+' class="img-fluid" width="32" height="32" alt="" align="middle"></a></div></td>'																										           
        params["comentarios_recentes"].append(elem)
    params["header_comentarios_recentes"] = ['Comentário', 'Tendência', 'Ver publicação']
    
    params["qnt_frases_recentes"] = len(comentarios_recentes)
    qnt_frases_recentes_positivas = len([p for p in comentarios_recentes if  p["aumentos"]>p["quedas"]])
    qnt_frases_recentes_negativas = len([p for p in comentarios_recentes if  p["aumentos"]<=p["quedas"]])
    params["valores_donut_comentarios"] = [{'value':qnt_frases_recentes_positivas, 'name':"Positivas"}, {'value':qnt_frases_recentes_negativas, 'name':"Negativas"}]


    analise_semantica = ResultadosRepository().recuperar_analise_semantica()
    total_positivos = analise_semantica["fp"]+analise_semantica["tp"]
    total_negativo = analise_semantica["tn"]+analise_semantica["fn"]
    params["analise_semantica_f_negativo"] = analise_semantica["fn"]/total_negativo
    params["analise_semantica_f_positivo"] = analise_semantica["fp"]/total_positivos
    params["analise_semantica_v_negativo"] = analise_semantica["tn"]/total_negativo
    params["analise_semantica_v_positivo"] = analise_semantica["tp"]/total_positivos
    params["analise_semantica_precisao"] = analise_semantica["precision"] 
    params["analise_semantica_resultado_icone"] = 'icon-stars'
    if analise_semantica["previsao"]==1:
        params["analise_semantica_resultado_cor"] = '#1B8A6B'
        params["analise_semantica_resultado_subtitulo"] = 'Avaliação Positiva. Estimativa de alta de pelo menos 5%'
    else:
        params["analise_semantica_resultado_cor"] = '#EF5350'
        params["analise_semantica_resultado_subtitulo"] = 'Avaliação Negativa. Estimativa de queda de pelo menos 5%'
    params["analise_semantica_resultado_titulo"] = 'RESULTADO'
    return render(request, 'dashboard.html', params)

def sobre_o_projeto(request):
    return render(request, 'sobre_o_projeto.html', dict())

def contato(request):
    return render(request, 'contato.html', dict())

def apresentacao(request):
    return render(request, 'apresentacao.html', dict())

def padroes_numericos(request):
    return render(request, 'padroes_numericos.html', dict())

def repeticao_comentarios(request):
    return render(request, 'repeticao_comentarios.html', dict())

def analise_semantica(request):
    return render(request, 'analise_semantica.html', dict())