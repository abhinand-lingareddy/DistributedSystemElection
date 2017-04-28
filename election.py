from kazoo.client import KazooClient
import kazoo
from kazoo.client import KazooState

class election:
    def stat_listener(self,state):
        if state == KazooState.LOST:
            print 'Lost'
        elif state == KazooState.SUSPENDED:
            print 'Suspended'
        elif state == KazooState.CONNECTED:
            self.perform()
            print 'Connected'
        else:
            print 'Unknown state'

    def getmaster(self):
        return self.is_master


    def __init__(self, zk,path,value):
        self.zk=zk
        self.path=path
        self.value=value
        self.is_master=False
        self.child=None
        zk.add_listener(self.stat_listener)
        #creater a counter if not exsisting


    def find_parent(self,key):
        while(True):
            stat=self.zk.exists("parent/" + key)
            if stat==None:
                return None
            else:
                parent = self.zk.get("parent/" + key)
                key=str(parent[0])
            stat = self.zk.exists(key)
            if stat is not None:
                return key
    def key_string(self,num):
        return self.key+str(num).zfill(10)

    def my_func(self,data, stat):
        print "entered"
        if stat is None:
            print "called " + str(data)
            self.parentkey = self.find_parent(self.parentkey)
            if self.parentkey is None:
                self.is_master = True
            else:
                print "parent " + self.parentkey
                kazoo.recipe.watchers.DataWatch(self.zk, self.parentkey, func=self.my_func)
            return False
    def perform(self):
        lock = self.zk.Lock("/lockpath", self.value)

        with lock:
            self.key=str(self.zk.create( self.path, self.value, ephemeral=True, makepath=True,sequence=True))
            print self.key
            num=self.key[self.key.index(self.path)+len(self.path):]
            childnum=int(num)+1
            end=self.zk.exists("end")
            if end is None:
                self.zk.create("end",self.key)
                self.parentkey=None
            else:
                self.parentkey = str(self.zk.get("end")[0])
                self.zk.set("end", self.key)
                print "end "+self.parentkey
                self.zk.create("parent/" + self.key, self.parentkey, makepath=True)


            if self.parentkey is None:
                self.is_master=True
            else:
                self.is_master = False
                kazoo.recipe.watchers.DataWatch(self.zk, self.parentkey, func=self.my_func)


            # @kazoo.client.DataWatch(self.zk, self.parentkey)
            # def watch_Sucessor(data, stat):
            #     print "called on "+data
            #     if stat is None:
            #         print "called " + str(data)
            #         self.parentkey = self.find_parent(self.parentkey)
            #         if self.parentkey is None:
            #             self.is_master = True
            #         else:
            #             kazoo.recipe.watchers.DataWatch(self.zk, self.parentkey, func=watch_Sucessor)
            #         return False
        #release lock






