#!/usr/bin/env python

'''
Filtering schedule.txt to only include pid from certain pid list
USAGE:
    cat schedule.txt | ./filter_schedule.py pid_list.txt > filtered_schedule.txt

@author Jiayu Zhou

'''

import sys


def show_usage():
    print "USAGE: cat schedule.txt | ./filter_schedule.py pid_list.txt > filtered_schedule.txt"

def read_pid(pid_list_file):
    pid_list = set()
    with open(pid_list_file) as file_content:
        for line in file_content:
            line = line.strip()
            pid_list.add(line)
    return pid_list

if __name__ == "__main__":
    if len(sys.argv) < 2:
        show_usage()
    else:
        pid_list_file = sys.argv[1]

        # read pid list
        pid_list = read_pid(pid_list_file)
        sys.stderr.write("The length of pid list read from file is " + str(len(pid_list)) + "\n")

        # start to filtering input from stdio
        cnt = 0
        for line in sys.stdin:
            splits = line.split("|")
            if (splits[4] in pid_list):
                print line.strip()
                cnt = cnt + 1
        sys.stderr.write(str(cnt) + " lines in total after filtering. \n")

        sys.stderr.flush()


