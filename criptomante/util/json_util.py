import jsonpickle
from datetime import date, datetime
import enum
import decimal


class JsonUtil:
    def encode(self, dados: object):
        # Transforma o documento em um dicionario, ao inves de objeto
        dic = self.toDict(dados)
        return jsonpickle.encode(dic)

    def decode(self, dados: object):
        return jsonpickle.decode(dados)

    def toDict(self, dados: object):
        """
        Converte um Objeto em um dicionario. Caso seja um tipo primitivo, retorna o proprio argumento recebido
        """
        if (isinstance(dados, dict)):  # A entrada já foi um dict
            saida = dict()
            for key in dados.keys():
                saida[key] = self.toDict(dados[key])
            return saida
        if (isinstance(dados, list)):
            saida = list()
            for elem in dados:
                saida.append(self.toDict(elem))
            return saida
        if isinstance(dados, enum.Enum):
            return dados.value
        if (isinstance(dados, date)):  # Tratamento de datas
            return dados.strftime('%d/%m/%Y')
        if isinstance(dados, decimal.Decimal):  # Tratamento de decimal
            return float(dados)
        if (not hasattr(dados, "__dict__")):  # A entrada nao pode ser convertida para dict
            return dados
        else:
            saida = dict()  # A entrada foi um objeto, que deverá ser convertido para dict
            # Para cada atributo...
            for atributo in [a for a in dir(dados) if not a.startswith('__') and not callable(getattr(dados, a))]:
                atr = getattr(dados, atributo)
                saida[atributo] = self.toDict(atr)
            return saida


