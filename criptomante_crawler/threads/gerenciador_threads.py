from threading import Thread
from criptomante_crawler.threads.minha_thread import MinhaThread
from time import sleep
from datetime import datetime, timedelta

class GerenciadorThreads(MinhaThread):
    threads = dict()
    imprimir_progresso:bool
    classe_threads: type = MinhaThread
    a_iniciar = dict()
    
    @classmethod
    def iniciar_gerenciamento(cls, classe_threads, imprimir_progresso = True, quantidade_limite = 100, fabricar = False, timeout = None):
        GerenciadorThreads.threads[classe_threads.__name__]=list()
        GerenciadorThreads.a_iniciar[classe_threads.__name__]=list()
        classe_threads.esperando_novos = True
        classe_threads.quantidade_limite = quantidade_limite

        gerenciador = GerenciadorThreads()
        gerenciador.classe_threads = classe_threads
        gerenciador.imprimir_progresso = imprimir_progresso
        gerenciador.quantidade_limite = quantidade_limite
        gerenciador.fabricar = fabricar
        if timeout!=None:
            gerenciador.timeout = timedelta(seconds=timeout)
        else:
            gerenciador.timeout=None
        gerenciador.start()
    
    def registrar(self, thread:MinhaThread):
        nome_classe = thread.__class__.__name__
        if not nome_classe in GerenciadorThreads.threads:
            GerenciadorThreads.threads[nome_classe] = list()
        GerenciadorThreads.threads[nome_classe].append(thread)
        thread.hora_inicio = datetime.now()
        self.a_iniciar[self.classe_threads.__name__].append(thread)
    
    def executar(self):
        if (self.fabricar) and (self.classe_threads.esperando_novos):
            for t in self.classe_threads.fabricar(10000 ):
                self.registrar(t)
        else:
            self.parar()


        segundos =0
        while (True):
            segundos+=1
            self.resolverTimeout(self.classe_threads)
            threads_ativas, threads_totais, threads_finalizadas, threads_em_fila = self.threadsAtivas(self.classe_threads.__name__)

            #if (self.imprimir_progresso) and (segundos==5):
            print("Threads {}: {}/{} Concluídas".format(self.classe_threads.__name__,threads_finalizadas,threads_totais))
            #    segundos=0
            
            while (threads_ativas<self.classe_threads.quantidade_limite) and (threads_em_fila>0):
                a_iniciar = GerenciadorThreads.a_iniciar[self.classe_threads.__name__]
                if len(a_iniciar)>0:
                    t = a_iniciar[0]
                    del(a_iniciar[0])
                    threads_ativas+=1
                    threads_em_fila-=1
                    t.start()
            if (threads_ativas==0)  and (threads_em_fila==0):
                return
            sleep(1)
                    
                    
            

    @classmethod
    def threadsAtivas(cls, nome_classe):
        a_iniciar = GerenciadorThreads.a_iniciar[nome_classe]
        if not nome_classe in GerenciadorThreads.threads:
            return 0, 0, 0
        threads_totais = len(GerenciadorThreads.threads[nome_classe])
        threads_finalizadas = 0
        for t in GerenciadorThreads.threads[nome_classe]:
            if (not t.is_alive()) and (not t in a_iniciar):
                threads_finalizadas+=1
        return threads_totais - threads_finalizadas - len(a_iniciar), threads_totais, threads_finalizadas, len(a_iniciar)

    @classmethod
    def finalizar_gerenciamento(cls, classe_threads, continuar_esperando_novas_threads = False):
        classe_threads.esperando_novos = continuar_esperando_novas_threads
        ativas, totais, finalizadas, em_fila  = cls.threadsAtivas(classe_threads.__name__)
        while not (ativas==0):
            sleep(5)
            ativas, totais, finalizadas, em_fila = cls.threadsAtivas(classe_threads.__name__)
        print("Threads finalizadas: {}".format(classe_threads.__name__))
            
    def resolverTimeout(self, classe_threads):
        for t in GerenciadorThreads.threads[classe_threads.__name__]:
            if (self.timeout!=None)  and (t.hora_inicio + self.timeout < datetime.now()):
                t.parar() 
            if (self.timeout!=None) and (t.is_alive()) and (t.hora_inicio + (self.timeout*3) < datetime.now()):
                GerenciadorThreads.threads[classe_threads.__name__].remove(t)


            
            


        
