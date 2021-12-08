import hashlib
import string
import random
from Queue import Queue
from Pyro4 import expose


@expose
class Solver:
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers

    def solve(self):
        target, n, algorithms, charset_tags = self.read_input()

        charset = self.get_charset(charset_tags)
        st_ids, end_ids = [0] * n, [len(charset) - 1] * n

        # split initial bounds into disjoint sub bounds in the number of jobs
        amount_of_jobs = len(self.workers) * n * len(charset)
        charset_bounds = self.split_charset(st_ids, end_ids, amount_of_jobs)
        random.shuffle(charset_bounds)

        results_queue = Queue()  # to gather all results from workers

        # load all workers with jobs
        for worker in self.workers:
            # execute task on worker and put result to queue when future finishes running
            worker.brute(algorithms, target, charset, charset_bounds.pop()) \
                .then(lambda w_res: results_queue.put((w_res, worker)))

        # gather results of all jobs from queue
        while amount_of_jobs > 0:
            worker_result, worker = results_queue.get()
            if worker_result is not None:
                self.write_output('\n'.join([target, worker_result]))
                return

            # load recently freed worker with new job if there is one
            if len(charset_bounds) > 0:
                worker.brute(algorithms, target, charset, charset_bounds.pop()) \
                    .then(lambda w_res: results_queue.put((w_res, worker)))
            amount_of_jobs -= 1
        self.write_output('\n'.join([target, 'nothing found']))

    @staticmethod
    @expose
    def brute(hash_names, target, charset, bounds):
        """Brute hash chain digest with words from generator
        in given bounds for each symbol """
        generator = Solver.message_generator(charset, *bounds)
        for message in generator:
            digest = message
            for hash_name in hash_names:
                digest = hashlib.new(hash_name, digest.encode()).hexdigest()
            if digest == target:
                return message
        return None

    @staticmethod
    def message_generator(charset, start_ids, end_ids):
        """Generates all variants of strings in given bounds"""

        def next_message():
            for i in range(len(start_ids) - 1, -1, -1):
                if current_ids[i] < end_ids[i]:
                    current_ids[i] += 1
                    message[i] = charset[current_ids[i]]
                    return True
                else:
                    current_ids[i] = start_ids[i]
                    message[i] = charset[current_ids[i]]
            return False

        current_ids = start_ids[:]
        message = [charset[c_i] for c_i in current_ids]
        yield ''.join(message)
        while next_message():
            yield ''.join(message)

    @staticmethod
    def get_charset(tags):
        tag_dict = {'d': string.digits,
                    'l': string.ascii_lowercase,
                    'u': string.ascii_uppercase,
                    'p': string.punctuation}
        charset = ''.join([tag_dict[c] for c in tags if c in tag_dict])
        return charset if len(charset) > 0 else string.ascii_letters

    @staticmethod
    def split_charset(start_ids, end_ids, n):
        """Split initial bounds into disjoint sub bounds"""
        bounds = []
        for rank in range(n):
            temp_si, temp_ei = start_ids[:], end_ids[:]
            i, rpp, rem = 0, 0, 0
            temp_n = n
            while i < len(start_ids) and rpp <= 0 <= rank:
                r = temp_ei[i] - temp_si[i] + 1
                rpp = r // temp_n
                rem = r % temp_n - (rpp <= 0)
                ps = temp_si[i] + rpp * rank + min(rem, rank)
                pe = ps if rpp <= 0 else ps + rpp + (rem > rank) - 1
                temp_si[i], temp_ei[i] = ps, pe
                temp_n -= rem
                rank -= rem
                i += 1
            if rpp > 0 or rank <= 0:
                bounds.append((temp_si, temp_ei))
        return bounds

    def read_input(self):
        with open(self.input_file_name, 'r') as f:
            target = f.readline().rstrip()
            n = int(f.readline())
            algorithms = f.readline().rstrip().split()
            charset_tag = f.readline().rstrip()
            return target, n, algorithms, charset_tag

    def write_output(self, output):
        with open(self.output_file_name, mode='w') as f:
            f.write(output)
