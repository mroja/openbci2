
import sys
import asyncio
import traceback


def print_threads(title=None):
    postfix = '' if title is None else ' - ' + title
    print('Print threads begin{}'.format(postfix))
    for th in threading.enumerate():
        print(th)
        traceback.print_stack(sys._current_frames()[th.ident])
        print('')
    print('Print threads end{}'.format(postfix))


def print_asyncio_tasks(title=None):
    postfix = '' if title is None else ' - ' + title
    print('Print asyncio tasks begin{}'.format(postfix))
    for task in asyncio.Task.all_tasks():
        print(task)
        print('')
    print('Print asyncio tasks end{}'.format(postfix))
