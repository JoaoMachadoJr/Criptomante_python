from criptomante_crawler.threads.minha_thread import MinhaThread
from time import sleep
from multiprocessing import Process, Queue
from typing import List

class ThreadsGenericas(Process):
    lista_threads =list()
   
    
    @staticmethod
    def construir(funcao, parametros, max_workers=0, iniciar=True):
        nova = ThreadsGenericas()
        nova.queue = Queue()
        nova.funcao = funcao
        nova.parametros = parametros
        nova.retorno=None
        ThreadsGenericas.lista_threads.append(nova)
        if iniciar:
            if max_workers!=0:
                while ThreadsGenericas.quantidade_vivas()>max_workers:
                    sleep(1)
            nova.start()    

    def run(self):
        self.retorno = [self.funcao(elem) for elem in self.parametros]

        self.queue.put(self.retorno)
    
    @staticmethod
    def resultado():
        import gc
        gc.collect()
        saida = list()
      #  for t in ThreadsGenericas.lista_threads:
           # t.join()
        for t in ThreadsGenericas.lista_threads:
            saida = saida + t.queue.get()            
        ThreadsGenericas.lista_threads.clear()
        return saida
    
    @staticmethod
    def quantidade_vivas():
        saida = 0
        for t in ThreadsGenericas.lista_threads:
            if t.is_alive():
                saida+=1
        return saida

        