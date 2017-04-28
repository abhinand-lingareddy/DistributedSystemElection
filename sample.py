from kazoo.client import KazooClient
import election
import random
import time

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
port=random.randrange(49152, 65535)
e=election.election(zk,"xyz",str(port))
e.perform()
while(1):
    time.sleep(2)
    if e.getmaster():
        print e.getmaster()
