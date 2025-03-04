import time
from pig_util import outputSchema

@outputSchema('formatted_time:chararray')
def evaluate(epoch):
    try:
        new_epoch = int(epoch[:-3]) #convert to int
        formatted_time = time.strftime('%Y-%m-%d', time.localtime(new_epoch))
        return formatted_time
    except (ValueError, TypeError, IndexError) as e:
        return None # Handle errors gracefully.