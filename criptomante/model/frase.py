from datetime import datetime
class Frase:
    texto:str
    mensagem:str
    data:datetime

    def __init__(self, texto, mensagem, frase = None, data = None):
        self.texto = texto
        self.mensagem = mensagem
        self.frase = frase
        self.data_mensagem = data
