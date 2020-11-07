from django.shortcuts import render
from criptomante.repository.nprevisoes import NPrevisoesRepository
from criptomante.repository.resultados_repository import ResultadosRepository
from criptomante.repository.postagensRepository import PostagensRepository
from datetime import datetime, date
from criptomante.model.snapshot import Snapshot
from criptomante.util.json_util import JsonUtil

# Create your views here.
def dashboard(request):
    from django.templatetags.static import static
    
    params = dict()
    previsao_numerica = ResultadosRepository().listar_previsao_numerica('mediana')    
    params["qnt_frases_tendencia"]=5
    params["comentarios_recentes"] = list()
    comentarios_recentes = ResultadosRepository().listar_comentarios_recentes("Frase")
    for c in comentarios_recentes[0:100]:
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


    # Manipula previsao numérica
    
    params["transacoes"] = [ [p["data"].strftime("%Y-%m-%d"), float(p["transacoes"])] for p in previsao_numerica]
    if (qnt_frases_recentes_positivas>qnt_frases_recentes_negativas) and (analise_semantica["previsao"]==1):
        params["previsao_numerica"] = [ [p["data"].strftime("%Y-%m-%d"), float(p["valor"])*1.05] 
                                         if p["data"]>date.today() 
                                         else  [p["data"].strftime("%Y-%m-%d"), float(p["valor"])]  
                                         for p in previsao_numerica]
    
    elif (qnt_frases_recentes_positivas<qnt_frases_recentes_negativas) and (analise_semantica["previsao"]!=1):
        params["previsao_numerica"] = [ [p["data"].strftime("%Y-%m-%d"), float(p["valor"])*0.95] 
                                         if p["data"]>date.today() 
                                         else  [p["data"].strftime("%Y-%m-%d"), float(p["valor"])]  
                                         for p in previsao_numerica]
    else:
        params["previsao_numerica"] = [ [p["data"].strftime("%Y-%m-%d"), float(p["valor"])] for p in previsao_numerica]

    return render(request, 'dashboard.html', params)

def sobre_o_projeto(request):
    return render(request, 'sobre_o_projeto.html', dict())

def contato(request):
    return render(request, 'contato.html', dict())

def apresentacao(request):
    return render(request, 'apresentacao.html', dict())

def padroes_numericos(request):
    previsao_numerica = ResultadosRepository().listar_previsao_numerica('mediana')
    params = dict()
    params["grafico_historico"] = [ [p["data"].strftime("%Y-%m-%d"), float(p["valor"])] for p in previsao_numerica if p["data"]<=date.today() ]
    params["previsao_numerica"] = [ [p["data"].strftime("%Y-%m-%d"), float(p["valor"])] for p in previsao_numerica]

    #Snapshot atual
    snapshot_atual : Snapshot= ResultadosRepository().listar_snapshot_mais_recente()[0]
    params["header_snapshots"] = ["Sigla", "Data", "Variação", "Cotação na Data"]
    params["tabela_snapshot_atual"] = snapshot_to_tabela(snapshot_atual)

    #Melhores Snapshots
    snapshots = ResultadosRepository().listar_snapshot_maior_pontuacao()
    snapshots = sorted(snapshots, key=lambda x: x.pontuacao, reverse=True)
    params["tabela_melhor_snapshot_1"] = snapshot_to_tabela(snapshots[0],snapshot_atual)
    params["tabela_melhor_snapshot_2"] = snapshot_to_tabela(snapshots[1],snapshot_atual)
    params["tabela_melhor_snapshot_3"] = snapshot_to_tabela(snapshots[2],snapshot_atual)

    params["Pontuacao1"] = snapshots[0].pontuacao
    params["Pontuacao2"] = snapshots[1].pontuacao
    params["Peso1"] = pow(snapshots[0].pontuacao,snapshots[0].pontuacao)
    params["Peso2"] = pow(snapshots[1].pontuacao,snapshots[1].pontuacao)

    params["header_tabela_lista"] = ["Data Referência do Snapshot",  "Cotação em DR", "Cotação depois de 1 Mês", "Variação","Peso"]
    params["tabela_lista"] = list()
    for s in snapshots[0:10]:
        linha = dict()
        linha["Data Referência do Snapshot"]="<td>"+s.data.strftime("%d/%m/%Y")
        linha["Peso"] = "<td>"+str(pow(s.pontuacao,s.pontuacao))
        linha["Cotação em DR"] = "<td>"+"$ "+str(round(s.momentos["DR"].valor,2)).replace(".",",")
        linha["Cotação depois de 1 Mês"] = "<td>"+"$ "+str(round(s.momentos["DR+1M"].valor,2)).replace(".",",")
        linha["Variação"] = "<td>"+str(round(s.momentos["DR+1M"].variacao*100,2))+"%"
        params["tabela_lista"].append(linha)

    return render(request, 'padroes_numericos.html', params)

def repeticao_comentarios(request):
    from django.templatetags.static import static
    from criptomante.analyzers.analyzer_textual import AnalyzerTextual
    import  en_core_web_sm
    nlp = en_core_web_sm.load()
    mensagem_com_mais_frases = None
    maior_quantidade_frases = 0

    params = dict()
    mensagens_recentes = PostagensRepository().listar_mensagens_recentes_ordenadas_pelo_horario()
    params["mensagens_recentes"] = [{"Mensagem": '<td style="width: 70%" ><div class="d-flex align-items-center"><div><div class="text-default font-size-md"> '+m["texto"]+"</div></div> </div></td>",
                                    "Publicação":'<td><div class="d-flex justify-content-center"><a href="'+m["topico_url"]+'"><img src="{url}"'.format(url=static('images/reddit.png'))+' class="img-fluid" width="32" height="32" alt="" align="middle"></a></div></td>'}
                                    for m in mensagens_recentes]
    params["mensagens_recentes_header"] = ["Mensagem", "Publicação"]

    for m in mensagens_recentes:
        doc = nlp(m["texto"])
        sents = [s for s in doc.sents]
        if len(sents) > maior_quantidade_frases:
            maior_quantidade_frases = len(sents)
            mensagem_com_mais_frases = m

    params["mensagem_mais_frases"] ='"'+ mensagem_com_mais_frases["texto"]+'"'
    doc = nlp(mensagem_com_mais_frases["texto"])
    sents = [s for s in doc.sents]
    
    params["frases"] = '   '.join(['<li><span style="text-indent: -24px;">' + str(s) + '</a></span></li>' for s in sents])

    frases_tratadas = [myStrip(sent.string) for sent in doc.sents]
    frases_tratadas = [f for f in frases_tratadas if f!="" and not "http" in f]
    params["frases_pos_tratamento"] = '   '.join(['<li><span style="text-indent: -24px;">' + str(f) + '</a></span></li>' for f in frases_tratadas])


    comentarios_recentes = ResultadosRepository().listar_comentarios_recentes("Frase")
    params["comentarios_recentes"]=[]
    for c in comentarios_recentes[0:100]:
        elem = dict()
        elem["Frase"] = '<td style="width: 70%" ><div class="d-flex align-items-center"><div><div class="text-default font-size-md"> '+c["frase"]+"</div></div> </div></td>"
        if c["aumentos"]>c["quedas"]:
            elem["Tendência"] =  '<td><span class="text-success-600"><i class="icon-stats-growth2 mr-2"></i> '+"{:3.2f}".format(((c["aumentos"]/ (c["aumentos"]+c["quedas"]))*100))+'%</span></td>'
        else:
            elem["Tendência"] =  '<td><span class="text-danger"><i class="icon-stats-decline2 mr-2"></i> '+"{:3.2f}".format(((c["quedas"]/ (c["aumentos"]+c["quedas"]))*100))+'%</span></td>'
        elem["Vezes em que precedeu uma queda"] = '<td style="width: 70%" ><div class="d-flex align-items-center"><div><div class="text-default font-size-md"> '+str(c["quedas"])+"</div></div> </div></td>"
        elem["Vezes em que precedeu um aumento"] = '<td style="width: 70%" ><div class="d-flex align-items-center"><div><div class="text-default font-size-md"> '+str(c["aumentos"])+"</div></div> </div></td>"
        params["comentarios_recentes"].append(elem)
    params["header_comentarios_recentes"] = ['Frase', "Vezes em que precedeu uma queda", "Vezes em que precedeu um aumento", 'Tendência']

    return render(request, 'repeticao_comentarios.html', params)

def analise_semantica(request):
    params = dict()
    analise_semantica = ResultadosRepository().recuperar_analise_semantica()
    total_positivos = analise_semantica["fp"]+analise_semantica["tp"]
    total_negativo = analise_semantica["tn"]+analise_semantica["fn"]
    params["analise_semantica_f_negativo"] = str(round((analise_semantica["fn"]/total_negativo) * (100),2))+'%'
    params["analise_semantica_f_positivo"] = str(round((analise_semantica["fp"]/total_positivos) * (100),2))+'%'
    params["analise_semantica_v_negativo"] = str(round((analise_semantica["tn"]/total_negativo) * (100),2))+'%'
    params["analise_semantica_v_positivo"] = str(round((analise_semantica["tp"]/total_positivos) * (100),2))+'%'
    params["analise_semantica_precisao"] = str(round((analise_semantica["precision"] )* (100),2))+'%'

    frases_recentes = PostagensRepository().listar_frases_recentes_antes_e_depois_de_tratamento()
    params["frases_recentes"] = [{"Frase pós-tratamento":'<td style="width: 70%" ><div class="d-flex align-items-center"><div><div class="text-default font-size-md"> '+frase["texto_tratado"]+"</div></div> </div></td>", 
                                  'Frase antes do tratamento' : '<td style="width: 70%" ><div class="d-flex align-items-center"><div><div class="text-default font-size-md"> '+frase["texto"]+"</div></div> </div></td>"} for frase in frases_recentes[0:15]]
    params["header_frases_recentes"] = ['Frase antes do tratamento', 'Frase pós-tratamento']

    params["analise_semantica_resultado_icone"] = 'icon-stars'
    if analise_semantica["previsao"]==1:
        params["analise_semantica_resultado_cor"] = '#1B8A6B'
        params["analise_semantica_resultado_subtitulo"] = 'Avaliação Positiva. Estimativa de alta de pelo menos 5%'
    else:
        params["analise_semantica_resultado_cor"] = '#EF5350'
        params["analise_semantica_resultado_subtitulo"] = 'Avaliação Negativa. Estimativa de queda de pelo menos 5%'
    params["analise_semantica_resultado_titulo"] = 'RESULTADO'
    return render(request, 'analise_semantica.html', params)

def snapshot_to_tabela(snapshot:Snapshot, snapshot_comparativo:Snapshot = None):
    saida = list()
    for m in snapshot.momentos_passado().keys():
        linha = dict()
        if (snapshot_comparativo == None) or (abs(snapshot.momentos[m].variacao/snapshot_comparativo.momentos[m].variacao)>1.1) or (abs(snapshot.momentos[m].variacao/snapshot_comparativo.momentos[m].variacao)<0.9):
            linha["Sigla"] = "<td>"+m
            linha["Data"] = "<td>"+snapshot.momentos[m].data.strftime("%d/%m/%Y")
            linha["Cotação na Data"] = "<td>$ "+ str(round(snapshot.momentos[m].valor,2)).replace(".",",")
            linha["Variação"] = "<td>"+ str(round(snapshot.momentos[m].variacao*100,2))+"%"
        else:
            linha["Sigla"] = "<td><b>"+m+"</b>"
            linha["Data"] = "<td><b>"+snapshot.momentos[m].data.strftime("%d/%m/%Y")+"</b>"
            linha["Cotação na Data"] = "<td><b>$ "+ str(round(snapshot.momentos[m].valor,2)).replace(".",",")+"</b>"
            linha["Variação"] = "<td><b>"+ str(round(snapshot.momentos[m].variacao*100,2))+"%"+"</b>"
        linha["Data2"] = snapshot.momentos[m].data        
        saida.append(linha)
        saida = sorted(saida, key=lambda x:x["Data2"], reverse=True)

    
    return saida

def myStrip(frase:str):
        CARACTERES_A_SEREM_PERMITIDOS_NO_INICIO = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        CARACTERES_A_SEREM_PERMITIDOS_NO_FIM = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.!?,;"
        while (len(frase)>0) and (frase[0] not in CARACTERES_A_SEREM_PERMITIDOS_NO_INICIO):
            frase = frase[1:len(frase)]
        while (len(frase)>0) and (frase[len(frase)-1] not in CARACTERES_A_SEREM_PERMITIDOS_NO_FIM):
            frase = frase[0:len(frase)-1]
        return frase 
if __name__ == "__main__":
    dashboard(None)

