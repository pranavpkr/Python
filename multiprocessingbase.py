from multiprocessing import Process, Lock
from time import sleep
from random import random
from datetime import datetime
from sys import argv

lck = Lock()

def work(nums,i):
    global lck
#    sleep( random() )
    with open('mp.txt', 'a') as op:
        for n in nums:
            lck.acquire()
            op.write( n+','+i+'\n' )
            print(n,i)
            sleep(0.1)
            lck.release()

if __name__ == "__main__": 
    # creating processes
    
    nofp = int(argv[1]) if len( argv ) > 1 else 2
    l = int(argv[2]) if len( argv ) > 2 else 10
    
    nums =[ str(i) for i in range(l)]

    process =[];ch = int(l/nofp)
    print( 'No of thds:', nofp, '\nChunk size:', ch, '\nLgth of no:', l)
    
    with open('mp.txt', 'w') as op:
#        op.write( ','.join(nums)+'\n' )
            pass
    
    for i in range(nofp):
        last = l if i == nofp-1 else ch*(i+1)
        process.append( Process(target=work, args=( nums[ ch*i : last ],str(i) ))  )

    var = datetime.now()
    for p in process:
        p.start()

    for p in process:
        p.join()

    print("mp done in", datetime.now() - var)

#    var = datetime.now()
#    with open('srl.txt', 'w') as sr:
##        sr.write( ','.join(nums)+'\n' )
#        for n in nums:
#            sr.write( n+',1'+'\n' )
#            sleep(0.1)
#    print("sg done in", datetime.now() - var)
