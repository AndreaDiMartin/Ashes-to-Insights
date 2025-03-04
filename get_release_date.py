import time

def get(epoch):
    new_epoch = epoch[:-3]
    formatted_time = time.strftime('%Y %m %d', time.localtime(new_epoch))
    print(formatted_time)
    return formatted_time


