import os
from selenium import webdriver
import random
import requests
import gc
import wget
from random import randrange
import download

class Browser:

    @classmethod
    def lerHtml(cls, url:str)->str:
        headers = {
            'User-Agent': 'My User Agent {}'.format(randrange(1000))
           
        }
        r = requests.get(url, headers=headers)        
        html = r.text
        r.close()
        del(r)
        gc.collect()
        return html

    @classmethod
    def download(cls, url:str, caminho_destino:str)->str:
        if os.path.exists(caminho_destino):
            os.remove(caminho_destino)
        wget.download(url, caminho_destino)



    @classmethod
    def lerHtmlComJs(cls, url:str) -> str:        
        browser = cls.get_chrome()
        try:
            
            browser.get(url)
            code = browser.page_source
        except:
            code = ''
        finally:
            browser.stop_client()
            browser.close()
            browser.quit()
            del (browser)
        gc.collect()
        return code

    
    @classmethod
    def get_chrome(cls) -> webdriver.chrome.webdriver.WebDriver:
        agent= 'My User Agent {}'.format(randrange(1000))
        _driver = os.getenv("driver_chrome", "D:\\Users\\Joao\\Downloads\\GoogleChromePortable\\chromedriver.exe")
        _options = webdriver.chrome.options.Options()
        _options.add_argument("--headless")
        _options.add_argument("--no-sandbox")
        _options.add_experimental_option('excludeSwitches', ['enable-logging'])
        _options.add_argument("--disable-notifications")
        _options.add_argument("user-agent="+agent)
        browser = webdriver.Chrome(options=_options, executable_path=_driver)
        browser.set_page_load_timeout(60)
        browser.set_window_size(1366, 768)
        return browser

    

if __name__ == "__main__":
    from datetime import datetime
    agora = datetime.now()
    print(agora)



