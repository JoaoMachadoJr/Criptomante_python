from typing import List
from threading import Thread
import time

class ThreadUtil:
    threads = dict()

    @classmethod
    def esperar(cls, lista: List[Thread], descricao_print = None):
        threads_totais = len(lista)
        threads_ativas = len(lista)
        tentativas = 0
        while (threads_ativas!=0):
            tentativas +=1
            threads_ativas=0
            for t in lista:
                if t.is_alive():
                    if tentativas==30:
                        break
                    else:
                        threads_ativas=threads_ativas+1
            if (descricao_print != None):
                print("Threads "+descricao_print+": "+str(threads_ativas)+"/"+str(threads_totais))
            time.sleep(10)
        lista.clear()
