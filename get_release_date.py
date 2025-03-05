import time
from pig_util import outputSchema

@outputSchema('year:chararray')
def get_year(epoch):
    try:
        new_epoch = int(epoch[:-3])
        year = time.strftime('%Y', time.localtime(new_epoch))
        return year
    except (ValueError, TypeError, IndexError) as e:
        return None 

@outputSchema('month:chararray')
def get_month(epoch):
    try:
        new_epoch = int(epoch[:-3])
        month = time.strftime('%m', time.localtime(new_epoch))
        return month
    except (ValueError, TypeError, IndexError) as e:
        return None 

@outputSchema('day:chararray')
def get_day(epoch):
    try:
        new_epoch = int(epoch[:-3])
        day = time.strftime('%d', time.localtime(new_epoch))
        return day
    except (ValueError, TypeError, IndexError) as e:
        return None 

@outputSchema('day_of_week:chararray')
def get_day_of_week(epoch):
    try:
        new_epoch = int(epoch[:-3])
        day_of_week = time.strftime('%a', time.localtime(new_epoch))
        return day_of_week
    except (ValueError, TypeError, IndexError) as e:
        return None 