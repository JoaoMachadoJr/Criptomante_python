from datetime import datetime
from criptomante.repository.abstract_repository import AbstractRepository
from criptomante.model.nprevisao import NPrevisao
from typing import List
class NPrevisoesRepository(AbstractRepository):
		data: datetime
		confianca : int
		label:str
		valor:float
		
		
		def listar(self, confianca) -> List[NPrevisao]:
			saida =list()
			analise_textual = self.resultadoAnaliseTextual()
			
			sql = "select data, confianca, label, valor from temp.nprevisao where confianca=:confianca order by data;"
			params = dict()
			params["confianca"] = confianca
			result = self.fetchAll(sql, params)
									
			for previsao in result:
				prev = NPrevisao()
				prev.data = previsao["data"]
				prev.confianca = previsao["confianca"]
				prev.label = previsao["label"]
				if (analise_textual>1.1):
					prev.valor = previsao["valor"]*float(1.03)
				elif (analise_textual<=0.9):
					prev.valor = previsao["valor"]*float(0.97)
				else:
					prev.valor = previsao["valor"]
				saida.append(prev)
			
			return saida
		


		
		
		
		def datas(self, lista):
			return  ",".join([elem.data.strftime('%d/%m/%Y') for elem in lista])					
		
		def valores(self, lista):
			return  ",".join([str(elem.valor) for elem in lista])				
				
		def labels(self, lista):
			return  ",".join(["'"+str(elem.label)+"'" for elem in lista])
		
		def limite_inferior(self, lista):
			saida = 99999999999
			divisor = 1
			
			for elem in lista:
			    if (elem.valor < saida):
			    	saida = elem.valor
			    
			
			saida = float(saida)* float(0.9)
			while ((saida//100)>=10):
				divisor = divisor*10
				saida = saida//100
			
			return saida * divisor
