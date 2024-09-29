#date utils 
from datetime import datetime, timedelta
class DLDateUtils:
    def __init__(self):
        pass

    def get_current_date(format='%Y%m%d'):
        return datetime.now().strftime(format)
    
    def get_previous_date(format='%Y%m%d'):
        return (datetime.now() - timedelta(days=1)).strftime(format)
    
    def get_next_date(format='%Y%m%d'):
        return (datetime.now() + timedelta(days=1)).strftime(format)
    