from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import time
import datetime

@ray.remote(resources={"Node1": 1})
class A(object):
    def __init__(self):
        print("Actor A start...")

    def f(self):
        num_of_items = 100 * 1000

        b = B.remote(num_of_items)
        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        print("push, start " + time_str)
        for i in range(num_of_items):
            b.f.remote(i)
        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        print("push, end " + time_str)
        time.sleep(5)

@ray.remote(resources={"Node2": 1})
class B(object):
    def __init__(self, num_of_items):
        print("Actor B start...")
        self.num_of_items = num_of_items
        self.sum = 0

    def f(self, object_id):
        #ray.get(object_id)
        #print (object_id)
        self.sum += 1
        if (self.sum == 1) :
            time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
            print("read, start " + time_str)

        if (self.sum == self.num_of_items) :
            time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
            print("read, end " + time_str)
        if (self.sum % 10000 == 0) :
            time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
            print("read " + str(self.sum) + "  " + str(time_str))


    def get_sum(self):
        return self.sum


if __name__ == "__main__":
    ray.worker._init(start_ray_local=True, use_raylet=True, num_local_schedulers=2, resources=[
        {
            "Node1": 1,
        },
        {
            "Node2": 1,
        }])
    A.remote().f.remote()
    time.sleep(300)

