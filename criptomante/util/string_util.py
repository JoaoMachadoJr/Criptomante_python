class StringUtil:
    @staticmethod
    def str_between(string, antes, depois):        
        try:
            start = string.index( antes ) + len( antes )
            end = string.index( depois, start )
            return string[start:end]
        except ValueError:
            return ""