import json

class ResumableController(object):
    class Env(object):
        def __init__(self, ident, is_seq):
            self.next_ident = 0
            self.ident = ident
            self.is_seq = is_seq
            self.tasks = []

    def __init__(self):
        self.levels = []
        self.saved_ident = None

    def next_ident(self):
        if not self.levels:
            return '0'
        last = self.levels[-1]
        current = last.next_ident
        last.next_ident += 1
        if last.is_seq:
            return "%s_%s" % (last.ident, current)
        else:
            return "%sp_%s" % (last.ident, current)

    def is_seq(self):
        if not self.levels:
            return True
        else:
            return self.levels[-1].is_seq

    def add_task(self, task):
        if not self.levels:
            return
        else:
            self.levels[-1].tasks.append(task)

    def tasks(self):
        return self.levels[-1].tasks


class BlockException(Exception):
    def __init__(self, code):
        super(BlockException, self).__init__()
        self.code = code

class RetryException(Exception):
    def __init__(self, stales):
        self.stales = stales

class ResumableExecutor(object):
    DONE = 0
    POSTED = 1
    FAILED = 2

    STATUS_STRING = ['done', 'posted', 'failed']

    def __init__(self, context=None):
        self.posts = {}
        self.tasks = []
        self.context = context
        self.control = ResumableController()

    class ExecuteManager(object):
        def __init__(self, control, sequence, name=None):
            self.control = control
            self.sequence = sequence
            self.name = name

        def __enter__(self):
            self.ident = self.control.next_ident()
            self.control.levels.append(self.control.Env(self.ident, self.sequence))

        def __exit__(self, exc_type, exc_value, *args):
            tasks = self.control.tasks()
            self.control.levels.pop()

            if exc_type not in [None, BlockException]:
                return False

            state = ResumableExecutor.DONE
            if self.sequence:
                if exc_type is BlockException:
                    state = exc_value.code
            else:
                failed = False
                for ident, local_state in tasks:
                    # any sub task under parallel is posted, then posted
                    if local_state == ResumableExecutor.POSTED:
                        state = ResumableExecutor.POSTED
                        break
                    elif local_state == ResumableExecutor.FAILED:
                        failed = True
                    state = local_state

                # any failed and no posted then failed
                if failed and state != ResumableExecutor.POSTED:
                    state = ResumableExecutor.FAILED

            if self.control.is_seq() and state != ResumableExecutor.DONE:
                raise BlockException(state)
            self.control.add_task((self.ident, state))
            return True

    def sequence(self, name=None):
        return self.ExecuteManager(self.control, True, name=name)

    def parallel(self, name=None):
        return self.ExecuteManager(self.control, False, name=name)

    def execute_function(self, fn):
        ident = self.control.next_ident()
        # if the context contains the id, it will assume that
        # the task has been executed, and the result has been updated in the context[id]
        # but when parallel tasks, there can be unfinished tasks.
        if ident in self.context:
            result = self.context[ident]
            if result['status'] == 'done':
                return json.loads(result['result']) if 'result' in result else None
            else:
                if result['status'] == 'failed':
                    state = ResumableExecutor.FAILED
                else:
                    state = ResumableExecutor.POSTED
                self.control.add_task((ident, state))
                if self.control.is_seq():
                    raise BlockException(state)
        else:
            # fn() just return the commands that will be executed.
            command = fn()
            self.posts[ident] = command
            self.control.add_task((ident, ResumableExecutor.POSTED))
            if self.control.is_seq():
                raise BlockException(ResumableExecutor.POSTED)
