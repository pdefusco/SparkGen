from datetime import date, timedelta, datetime
import re
import os.path


def daterange(start_date, end_date):
    """Create a date range generator 
    
    Args: 
        start_date (str): start date
        end_date (str): end date

    Returns: 
        a generator for date range
            
    """
    for n in range(int((end_date - start_date).days + 1)):
        yield start_date + timedelta(n)


def create_spark_date_filter(start_date, end_date): 
    """ Constructs a Spark Dataframe.filter() that will match the columns 'year','month','day' to the date range between the start_date (inclusive) and end_date arguments (inclusive)
    
    Args: 
        start_date (str):  "2021-6-1"
        end_date (str): "2021-6-2"

    Returns: 
        filter (str)    

    Example: 
        start_date =  date(2021, 6, 1) 
        end_date =  date(2021, 6, 2)
        returns '(year = 2021 and month = 06 and day = 01) or (year = 2021 and month = 06 and day = 02)'
    """
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    
    filter = ""
    # create a list of strings '(year == %Y and month == %m and day == %d)'
    times = [single_date.strftime('(year == %Y and month == %m and day == %d)') for single_date in daterange(start_date, end_date)]
    # join list
    filter = " or ".join(times)
    return filter
    
def interpret_input_date_arg(date_arg):
    """ Checks the date argument for special cases:
     - 'TODAY' is replaced with the current date
     - 'n_DAYS_AGO' is replaced the date n days ago

     Args: 
         date_arg (str): ideally "TODAY" or "n_DAYS_AGO"
     
     Return: 
         a date (str if no match, or date if it matches today's date or a relative date to today)
    """
    # check if it's supposed to be today's date
    if "TODAY".casefold() == str(date_arg).casefold():
        return date.today().strftime("%Y-%m-%d")

    # check for a relative date to today  
    n_days_ago = r'(\d+)_DAYS_AGO'
    match = re.match(n_days_ago, str(date_arg))
    if match:
        days = match.groups(1)[0]
        return datetime.strftime(date.today() - timedelta(int(days)), "%Y-%m-%d")
    
    # no special matches, return the original date string
    return date_arg

def get_version_number():
    if os.path.exists('version.yaml'): 
        version = open('version.yaml', 'r').read()
        if (len(version.strip()) != 0):
            return version
    return 'No version defined'

if __name__ == "__main__":
    pass