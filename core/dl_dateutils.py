# Audit Log
# Author: Sudheer Bommineni
# Email: sudheer.bommineni@kroger.com
# ID: KON8383
# Date: 2024-09-29
# Description: date utilities for the data lake

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
    