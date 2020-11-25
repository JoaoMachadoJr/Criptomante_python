 

def run():
    from dotenv import load_dotenv
    from pathlib import Path
    env_path = Path('C:\\@work\\pessoal\\Criptomante_python\\.env')
    load_dotenv(dotenv_path=env_path)

    import sys
    sys.path.append('C:\\@work\\pessoal\\Criptomante_python')
    from criptomante.analyzers.analyzer_numerico import AnalizerNumerico
    from criptomante.analyzers.analyzer_textual import AnalyzerTextual
    from criptomante.analyzers.registrador_de_resultados import RegistradorDeResultados
    from criptomante_crawler.crawlers.crawler_plataformas import CrawlerPlataformas
    from criptomante_crawler.crawlers.crawler_reddit import CrawlerReddit
    from criptomante.repository.postagensRepository import PostagensRepository
    import time
    
    print("Inicio do job")
    repositorio = PostagensRepository()
    repositorio.limparExecutores()
    print("Execucoes anteriores limpas")
    ultima = repositorio.obtemPostagemMaisRecente(100)
    if ultima==None:
        crawler_reddit = CrawlerReddit("https://api.pushshift.io/reddit/search/submission/?subreddit=Bitcoin&sort_type=created_utc&sort=asc")
    else:
        crawler_reddit = CrawlerReddit("https://api.pushshift.io/reddit/search/submission/?subreddit=Bitcoin&sort_type=created_utc&sort=asc&after={}".format(int(time.mktime(ultima.data.timetuple()))))
    crawler_reddit.website = "reddit.com/r/Bitcoin"
    print("Iniciando crawler")
    crawler_reddit.navegar()
    print("Finalizando crawler")

    time.sleep(120)
    print("Iniciando consulta a plataformas")
    crawler_numerico = CrawlerPlataformas()
    crawler_numerico.consultar_todas_as_plataformas()

    print("Iniciando analises numericas")
    numerico = AnalizerNumerico()
    numerico.analisar()

    print("Iniciando analises textuais ")
    textual = AnalyzerTextual()
    textual.analisar()

    print("Iniciando registrador de resultados")
    resultado = RegistradorDeResultados()
    resultado.registrar_resultados()

    print("Finalizando job")

    





if __name__ == "__main__":
    try:
        run()
    except expression as identifier:
        print("ERRO GRAVE NO JOB!")
        print(str(e))   
    