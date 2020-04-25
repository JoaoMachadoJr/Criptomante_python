from django.urls import path
from website import views

urlpatterns = [
    path('', views.dashboard, name='Dashboard'),
    path('projeto', views.sobre_o_projeto, name='Sobre o Projeto'),
    path('contato', views.contato, name='Contato'),
    path('apresentacao', views.apresentacao, name='Apresentação'),
    path('padroes_numericos', views.padroes_numericos, name='Análise por padrões numéricos'),
    path('repeticao_comentarios', views.repeticao_comentarios, name='Análise por repetição de comentários'),
    path('analise_semantica', views.analise_semantica, name='Análise por análise semântica de comentários'),
]