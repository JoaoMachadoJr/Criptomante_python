 

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
    
    repositorio = PostagensRepository()
    repositorio.limparExecutores()
    ultima = repositorio.obtemPostagemMaisRecente(100)
    if ultima==None:
        crawler_reddit = CrawlerReddit("https://api.pushshift.io/reddit/search/submission/?subreddit=Bitcoin&sort_type=created_utc&sort=asc")
    else:
        crawler_reddit = CrawlerReddit("https://api.pushshift.io/reddit/search/submission/?subreddit=Bitcoin&sort_type=created_utc&sort=asc&after={}".format(int(time.mktime(ultima.data.timetuple()))))
    crawler_reddit.website = "reddit.com/r/Bitcoin"
    crawler_reddit.navegar()
    

    crawler_numerico = CrawlerPlataformas()
    crawler_numerico.consultar_todas_as_plataformas()

    numerico = AnalizerNumerico()
    numerico.analisar()

    textual = AnalyzerTextual()
    textual.analisar()

    resultado = RegistradorDeResultados()
    resultado.registrar_resultados()

    





if __name__ == "__main__":
    run()       
    