import sys
import log

from ctypes import c_bool
from string import Template
from multiprocessing import Process, Manager, Queue, Value, active_children


headers = ['object', 'color']
template = Template('This is a $object. Some $object are $color')
#data.txt - comma separated values (e.g: 'guitar,red')
#result.txt - the merged result (e.g 'This is a guitar. Some guitar are red')

def combine(line):
    data = line.split(',')
    if len(data) > len(headers):
        log.error('more fields than headers to combine')
        raise IndexError
    if len(headers) > len(data):
        log.error('more headers than fields to combine')
        raise IndexError
    combs = []
    for x in range(len(headers)):
        combs.append((headers[x].strip(), data[x].strip()))
        if x is (len(headers) - 1):
            return template.substitute({k: v for k, v in combs})


def read(q, read_flag):
    log.debug('started reader')
    with open(file='data.txt', encoding='utf-8', mode='r') as d:
        lines = d.readlines()
        for line in lines:
            while q.qsize() > 10:
                continue
            q.put(line.strip('\n'))
    log.debug('reader done')
    if read_flag.value:
        read_flag.value = False


def watcher(q, wq, read_flag, write_flag):
    log.debug('started watcher')
    while q.qsize() is not 0 or read_flag.value:
        if q.qsize() is 0:
            continue
        else:
            while wq.qsize() > 10:
                continue
            wq.put(combine(q.get()), lock=True)
    log.debug('watcher done')
    if write_flag.value:
        write_flag.value = False


def write(wq, write_flag, read_flag):
    log.debug('started writer')
    with open(file='result.txt', encoding='utf-8', mode='w') as w:
        while wq.qsize() is not 0 or read_flag.value or write_flag.value:
            if wq.qsize() is 0:
                continue
            else:
                w.write(wq.get()+'\n')
        w.flush()
    log.debug('writer done')


def handler():
    try:
        with Manager() as m:
            thread_n = 4
            write_flag = Value(c_bool, True, lock=True)
            read_flag = Value(c_bool, True, lock=True)

            q = m.Queue()
            wq = m.Queue()
            r = Process(name='reader', daemon=True, target=read,
                        args=(q, read_flag, ))
            w = Process(name='writer', daemon=True, target=write,
                        args=(wq, write_flag, read_flag,))

            jobs = [Process(name=f'watcher_{i}', daemon=True, target=watcher, args=(q, wq, read_flag, write_flag,))
                    for i in range(thread_n)]
            r.start()
            w.start()
            for job in jobs:
                job.start()

            r.join()

            # force all the processes to terminate
            for prc in active_children():
                prc.terminate()
            w.join()

            for job in jobs:
                if job.is_alive():
                    log.debug(f'{job.name} - {job.pid} is hanging')
                job.join()

            # show processes status
            log.debug(r)
            for job in jobs:
                log.debug(job)
            log.debug(w)

    except Exception as e:
        log.error(e)
    finally:
        # exit app
        sys.exit()


if __name__ == "__main__":
    handler()
