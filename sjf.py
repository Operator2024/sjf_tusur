import threading
import time
from itertools import chain
from os import remove
from random import randint

import numpy as np


class MyThread(threading.Thread):
    '''
    Thread which can contain state: stopped or not
    Inherits from threading.Thread and doesn't start differ much
    '''

    def __init__(self, *args, **kwargs):
        super(MyThread, self).__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def stop(self, locks):
        '''
        Release executing student and set process state to stopped
        '''
        locks[self.name.split('_')[1]].release()
        self._stop_event.set()

    def is_stopped(self):
        '''
        Return state
        '''
        return self._stop_event.is_set()


class Professor:
    '''
    Implements professor - resource
    '''

    def __init__(self, data):
        '''
        Parameters are sent in a list or tuple
        Parameters: name, discipline and number of students to be executed in parallel
        '''
        self.name, self.discipline, self.students = data

    def __repr__(self):
        return '{}   {}   {}'.format(self.name, self.discipline, self.students)

    def __str__(self):
        return '{}\n{}\n{}'.format(self.name, self.discipline, self.students)


class Student:
    '''
    Implements student - process
    '''

    def __init__(self, data):
        '''
        Parameters are sent in list or tuple
        Parameters: name, group, discipline, burst, priority and arrival time
        '''
        self.name, self.group, self.discipline, self.burst, self.prior, self.arr_time = data
        self.rem_time = self.burst

    def __repr__(self):
        return '{}#{}#{}#{}#{}#{}'.format(
            self.name, self.group, self.discipline, self.burst, self.prior, self.arr_time)

    def __str__(self):
        return ('{}\n{}\n' + '{} ' * len(self.discipline) + '\n{}\n{}\n{}').format(
            self.name, self.group, *self.discipline, self.burst * 1000, self.prior, self.arr_time * 1000)

    def process(self, locks):
        '''
        Process function
        Locks student and sleeps for burst seconds
        '''
        locks[self.name].acquire()
        time.sleep(self.burst)
        if not threading.current_thread().is_stopped():
            locks[self.name].release()


class SJFnp:
    def __init__(self, of, prof, stud, q):
        '''
        state is:
            -1 if student didn'start come yet
            0 if students wait for professor
            1 if student is examinated
            2 if student finished his work
        self.state - state of each student
        self.allstud - students to arrive later 0
        self.qbegin - students arrived at the beginning
        self.delta - curr time / quant time
        self.q = quant time
        self.starttime - time of start main stage
        self.locks - lock thread for each student
        self.proc - dictionary of discipline and thread, ex. {"D1": [0,0]}
        self.now - dict with {"D1": [remaining time, priority]}
        '''
        self.of = of
        self.proc = {e.discipline: [] for e in prof}  # dict of disc : [cores]

        for e in prof:
            self.proc[e.discipline].extend(
                (
                    MyThread(target=None, name=e.name + '_ _ ') for _ in range(e.students)
                ))

        self.now = {e.discipline: [[0, 0] for _ in range(e.students)] for e in prof}

        self.state = {s.name: -1 for s in stud}
        self.allstud = [s for s in stud if s.arr_time != 0]
        self.qbegin = [s for s in stud if s.arr_time == 0]
        self.delta = 0
        self.q = q
        self.starttime = None
        self.locks = {s.name: threading.Lock() for s in stud}  # lock for each student
        self.prepare()

    def get_classname(self):
        return self.__class__.__name__

    def exec(self):
        '''
        This func receive all arriving students and processing all students in queue
        '''

        for k, s in enumerate(self.allstud):
            if time.time() - self.starttime > s.arr_time:
                self.qbegin.append(s)
                self.state[s.name] = 0
                self.allstud[k] = None
        self.prepare()

        for k, stud in enumerate(self.qbegin):
            if self.locks[stud.name].locked() is True:
                continue
            for j, d in enumerate(stud.discipline):
                i = self.find_alive(d)
                if i is not None:
                    self.state[stud.name] = 1
                    self.proc[d][i] = MyThread(target=stud.process, args=(self.locks,), name=self.proc[d][i].name.split('_')[0] + '_' + str(stud.name))
                    self.proc[d][i].start()
                    self.now[d][i] = [stud.rem_time, stud.prior]
                    del stud.discipline[j]
                    break

    def main(self):
        '''
        Processes all work for model
        '''

        self.starttime = time.time()
        self.tick(True)

        for p in self.proc.values():
            for pi in p:
                pi.start()

        while len(self.qbegin) > 0 or len(self.allstud) > 0:
            self.tick()
            self.exec()

        while any((pi.is_alive() for p in self.proc.values() for pi in p)):
            self.tick()
        time.sleep(self.q)
        self.tick(True)

    def prepare(self):
        '''
        Sort students by burst
        Check if some disciplines are not present among professors
        #
        '''
        self.qbegin = sorted(self.qbegin, key=lambda s: s.burst)
        if any((ei not in self.proc for e in self.qbegin for ei in e.discipline)):
            raise RuntimeError('Some disciplines are not present:( {}'.format(self.qbegin))

        self.allstud = [e for e in self.allstud if e is not None]
        self.qbegin = [e for e in self.qbegin if len(e.discipline) > 0]

    def tick(self, must=False):
        '''
        Find all processes finished their work
        Print state if next time quant went or must=True
        Decrement remaining time for all processes by q
        '''

        for pl in self.proc.values():

            for p in pl:
                if not p.is_alive():

                    pname = p.name.split('_')[1]

                    if (pname in self.state and pname not in chain(
                            (e.name for e in self.qbegin), (e.name.split('_')[1] for p in self.proc.values() for e in p if e.is_alive()))):
                        self.state[pname] = 2

        flag = time.time() - self.starttime > self.q * self.delta
        if must or flag:
            print(
                ('{:6}  ' * sum(len(e) for e in self.proc.values())).format(
                    *[pi.name.split('_')[1] if pi.is_alive() else '-1' for p in self.proc.values() for pi in p]) +
                ('{:3}' * len(self.state)).format(*self.state.values()),
                file=of)
            if flag:
                self.delta += 1
                for d in self.now:
                    for k in range(len(self.now[d])):
                        self.now[d][k][0] -= self.q

    def find_alive(self, d):
        '''
        loop through all processes for given discipline until finished found
        If such found, set corresponding student state to 2
        '''
        i = 0
        while self.proc[d][i].is_alive():
            i += 1
            if i == len(self.proc[d]):
                return None
        pname = self.proc[d][i].name.split('_')[1]
        if pname in self.state:
            self.state[pname] = 2
        return i


class SJFp(SJFnp):
    def exec(self):
        '''
        This method replaces the inherited to take into account priority
        Implements preemptive SJF with absolute priority
        '''

        for k, s in enumerate(self.allstud):
            if time.time() - self.starttime > s.arr_time:
                self.qbegin.append(s)
                self.state[s.name] = 0
                self.allstud[k] = None
        self.prepare()

        for k, stud in enumerate(self.qbegin):
            if self.locks[stud.name].locked():
                continue
            for j, d in enumerate(stud.discipline):
                i = self.find_alive(d)
                if i is None:
                    i = self.find_prior(d, stud.prior)
                if i is None:
                    i = self.find_burst(d, stud.prior, stud.burst)
                if i is not None:
                    self.state[stud.name] = 1
                    if self.proc[d][i].is_alive():
                        self.push_student(d, i)
                    self.proc[d][i] = MyThread(
                        target=stud.process,
                        args=(self.locks,),
                        name=self.proc[d][i].name.split('_')[0] + '_' + str(stud.name) + '_' + str(stud))
                    self.proc[d][i].start()
                    self.now[d][i] = [stud.rem_time, stud.prior]
                    del stud.discipline[j]
                    break

    def find_prior(self, d, p):
        '''
        Finds process for discipline d with lower priority < p
        '''
        i = 0
        while self.now[d][i][1] > p:
            i += 1
            if i == len(self.now[d]):
                return None
        return i

    def find_burst(self, d, p, b):
        '''
        Find process for discipline d with equal priority = p and bigger burst > b
        '''
        i = 0
        while self.now[d][i][0] < b or self.now[d][i][1] > p:
            i += 1
            if i == len(self.now[d]):
                return None
        return i

    def push_student(self, d, i):
        '''
        Push student back to queue
        '''
        self.proc[d][i].stop(self.locks)
        args = list(self.proc[d][i].name.split('_')[2].split('\n'))
        args[2] = [d]
        args[3] = self.now[d][i][0]
        args[4] = int(args[4])
        args[5] = float(args[5])
        name = args[0]
        self.qbegin.append(Student(args))


def read_data(of, fname='input.txt'):
    '''
    The func is reading data from input.txt file.
    If some data was omitted then they will be generated.
    Function returns class instance or SJFnp (sjf non-preemptive) either SJPp (sjf preemptive)
    '''
    with open(fname) as f:
        PA = int(f.readline().strip('\n'))
        QT = float(f.readline().strip('\n')) / 1000
        MaxT = int(f.readline().strip('\n'))
        MaxP = int(f.readline().strip('\n'))
        NR = int(f.readline().strip('\n'))
        ArgsR = [list(f.readline().strip('\n') for _ in range(3)) for i in range(NR)]
        NP = int(f.readline().strip('\n'))
        ArgsP = [list(f.readline().strip('\n') for _ in range(6)) for i in range(NP)]

    ArgsProf = list(map(lambda x: [str(x[0]), str(x[1]), int(x[2]) if x[2] != '' else ''], ArgsR))
    ArgsStud = list(map(lambda x: [
        str(x[0]),
        str(x[1]),
        x[2].split(),
        float(x[3]) / float(1000) if x[3] != '' else '',
        int(x[4]) if x[4] != '' else '',
        float(x[5]) / float(1000) if x[5] != '' else ''], ArgsP))
    #
    #
    # Exists names prof and disciplines:
    used_profnames = [e[0] for e in ArgsR]
    used_studnames = [e[0] for e in ArgsP]

    used_disc = np.unique([ds for d in (list(e[2]) for e in ArgsStud) for ds in d])
    if len(used_disc) == 0:
        used_disc = ['D1', 'D2', 'D3']

    profUniqueName = 1
    # Generate random data for professor
    for p in ArgsProf:
        if p[0] == '':
            while True:
                profName = 'P' + str(profUniqueName)
                if profName not in used_profnames:
                    break
            p[0] = profName
        if p[1] == '':
            p[1] = used_disc[randint(0, len(used_disc) - 1)]
        if p[2] == '':
            # Generate random count simultaneous students
            p[2] = randint(1, 4)
    Prof = list(map(Professor, ArgsProf))

    studentUniqueName = 1
    # Generate random data for student
    for p in ArgsStud:
        if p[0] == '':
            while True:
                studName = 'S' + str(studentUniqueName)
                studentUniqueName += 1
                if studName not in used_studnames:
                    break
            p[0] = studName
        if p[1] == '':
            p[1] = 'G1'
        if p[2] == '':
            p[2] = [used_disc[randint(0, len(used_disc) - 1)]]
        if p[3] == '':
            p[3] = randint(1, MaxT) / 1000
        if p[4] == '':
            p[4] = randint(1, MaxP)
        if p[5] == '':
            p[5] = randint(1, 5 * MaxT) / 1000
    Stud = [Student(arg) for arg in ArgsStud]

    print(NR, file=of)
    print(*map(str, Prof), sep='\n', file=of)
    print(NP, file=of)
    print(*map(str, Stud), sep='\n', file=of)
    print('STOP', file=of)
    # print(Prof)
    # print(Stud)

    if PA == 1:
        return SJFnp(of, Prof, Stud, QT)
    else:
        return SJFp(of, Prof, Stud, QT)


if __name__ == '__main__':
    of = open('temp_output.txt', 'w')
    try:
        # get schedule algo
        schedule = read_data(of)
        if schedule.get_classname() == "SJFnp":
            fname = "output_nonpreemptive.txt"
        elif schedule.get_classname() == "SJFp":
            fname = "output_preemptive.txt"
        else:
            raise Exception("Correct class name is not found")

        start = time.time()
        schedule.main()
        end = time.time() - start

    except Exception as e:
        print(e)
    finally:
        of.close()

    with open(f"output/{fname}", 'w') as of:
        with open('temp_output.txt', 'r') as f:
            for line in f.readlines():
                if line.strip() == 'STOP':
                    print(end, file=of)
                else:
                    print(line.strip(), file=of)
    remove('temp_output.txt')
