class CursorUtil:
    @staticmethod
    def fetchall(cursor):
        desc = cursor.description 
        return [
                dict(zip([col[0] for col in desc], row)) 
                for row in cursor.fetchall() 
        ]

    @staticmethod
    def fetchone(cursor):
        if cursor.rowcount==0:
            return None
        desc = cursor.description 
        return dict(zip([col[0] for col in desc], cursor.fetchone())) 
        