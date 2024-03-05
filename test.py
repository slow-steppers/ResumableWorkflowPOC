import time
import json
import asyncio
from functools import wraps, partial

import resumable_poc

executor = resumable_poc.ResumableExecutor()

def function(fn):
    def wrap(*args, **kwargs):
        cmd = (fn, args, kwargs)
        return executor.execute_function(lambda: cmd)
    return wrap

@function
def echo(message, data):
    print('echo', message)
    return data


def test():
    with executor.parallel():
        for x in range(2):
            with executor.sequence():
                a = echo(f'step-0-{x}', 1)
                b = echo(f'step-1-{x}', 1)
                with executor.parallel():
                    c = echo(f'step-2-0-{x}', x)
                    d = echo(f'step-2-1-{x}', c)
                e = echo(f'step-3-{x}', c + 1)

executor.context = {}
while True:
    try:
        test()
        break
    except resumable_poc.BlockException as ex:
        print('block:', ex)

    for k, v in executor.posts.items():
        print(f'exec:{k} {v}')
        executor.context[k] = {
            'result': json.dumps(v[0](*v[1], **v[2])),
            'status': 'done'
        }
