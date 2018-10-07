import requests as req
from multiprocessing import Process
from time import sleep

class SingerMorning:
    def __init__(self, app):
        self.app = app
        self.app.singer_morning = self
        self.process = {spider["name"]: False for spider in self.app.spiders}

    def ring_ring(self, delta):
        while True:
            req.get('https://scrappers-watchdog.herokuapp.com/')
            sleep(delta)

    def start_singing(self, spider_name):
        if self.process[spider_name]:
            self.process[spider_name].terminate()

        p = Process(target=self.ring_ring, args=(300,))
        self.process[spider_name] = p

    def end_singing(self, spider_name):
        if self.process[spider_name]:
            self.process[spider_name].terminate()

        self.process[spider_name] = False




