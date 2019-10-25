# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 00:21:51 2019

@author: pranav
"""
from multiprocessing import Process, Lock
from time import sleep
from random import random
from datetime import datetime
from sys import argv
from pandas import read_csv

delay = 0.08

#Line count = FINDSTR /R /N "^.*$" file.txt | FIND /C ":"
def work( idofp, chunkf,lck ):
    with open('mp.txt', 'a') as op:
        for i,r in chunkf.iterrows():
            sleep( delay )
            lck.acquire()
            op.write( str(i)+"|"+"|".join(r) + "\n" )
            lck.release()

if __name__ == "__main__": 
    # creating processes

    noofp = int(argv[1]) if len( argv ) > 1 else 4
    cz = int(argv[2]) if len( argv ) > 2 else 100
    print('No of threads',noofp )
    lock = Lock()
    processes =[]

    #open file    
    with open('mp.txt', 'w') as op: pass
    
    start = datetime.now()
    
    i=0
    for chunkf in read_csv('file.dsv', chunksize = cz, sep='|',dtype=str):
        i+=1
        print(i)#, end = '\r')

        processes.append( Process(target=work, args=( i, chunkf, lock, ))  )

        if len(processes) >= noofp:
            eval( "[p.start() for p in processes ]")
            eval( "[p.join() for p in processes ]")
            processes = []; i=0

    for remaingp in processes:remaingp.start()
    for remaingp in processes:remaingp.join()

    print( "MP done in", datetime.now()-start )
    
    '''
    start = datetime.now()
    df = read_csv('file.dsv', sep='|',dtype=str)
    with open('sp.txt', 'w') as op:
        for i,r in df.iterrows():
            sleep( delay )
            print( i, end = '\r')
            op.write( str(i)+"|"+"|".join(r) + "\n" )
    print( "SP done in", datetime.now()-start )
    '''