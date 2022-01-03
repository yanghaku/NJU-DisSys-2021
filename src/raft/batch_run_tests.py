"""
Copyright 2021-2022 Bo Yang <bo.yang@smail.nju.edu.cn>

this file is batch run "go test" to test raft in many times

"""

import argparse
import multiprocessing
import platform
import subprocess

from tqdm import tqdm


def work(cmd):
    fail_str = "FAIL"
    pass_str = "PASS"

    use_shell = platform.system() == "Windows"
    p = subprocess.Popen(cmd, shell=use_shell, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         encoding="utf-8")
    p.wait(timeout=1000)
    out, err = p.communicate()
    if out.find(fail_str) != -1 or err.find(fail_str) != -1 or out.find(pass_str) == -1:
        return out + err  # fail
    return pass_str


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-j", "--jobs", type=int, default=multiprocessing.cpu_count() * 2, help="process jobs")
    parser.add_argument("-t", "--total", type=int, default=1000, help="total test times")
    parser.add_argument("-r", "--run", default="", help="run the special test cases")
    parser.add_argument("-v", action='store_true', help="print the fail information")
    opt = parser.parse_args()
    
    total = opt.total
    command = ['go', 'test', '-race']
    if opt.run != "":
        command.append("-run")
        command.append(opt.run)

    print("run command = ", command)
    print("run with process pool, poll size =", opt.jobs, ", tasks =", total)
    pool = multiprocessing.Pool(processes=opt.jobs)

    res = [pool.apply_async(work, args=(command,)) for i in range(total)]
    pool.close()
    result = [i.get() for i in tqdm(res)]
    pool.join()

    passed = 0
    failed = 0
    for i in result:
        if i == 'PASS':
            passed += 1
        else:
            failed += 1
            if opt.v:
                print(i, "\n")

    print("total =", total, "passed =", passed, "failed =", failed)
    print("ok")
