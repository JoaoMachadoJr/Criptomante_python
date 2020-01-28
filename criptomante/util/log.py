from criptomante.repository.logRepository import LogRepository

class Log:

    @classmethod
    def info(cls, texto):
        repository = LogRepository()
        repository.insere(texto)
        print(texto)
