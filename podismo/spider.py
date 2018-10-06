import requests as req
from scrapy.selector import Selector
import csv
import sys
from time import sleep
import re
from math import floor
from os.path import isfile

init_url = 'https://www.calendariopodismo.it/index.php?startlimit=0&dove=0&regoprov=regione&tipogara=0&stringa=&dist=&luogosearch=&from=&to=&tipodata=tutto&ricercavanzata=avanzata&submit=+Cerca+#.W6u-4N9fjeT'

class Crawler:
    base_url = "https://www.calendariopodismo.it/index.php"
    base_race_url = "https://www.calendariopodismo.it/dettagli.php"

    race_params = {
        "idrecord": "0"
    }

    params = {
        "startlimit": 0,
        "dove": 0,
        "regoprov": "regione",
        "tipogara": "",
        "stringa": "",
        "dist": "",
        "luogosearch": "",
        "from": "",
        "to": "",
        "tipodata": "tutto",
        "ricercavanzata": "avanzata",
        "submit": "+Cerca+"
    }

    hash = ".W6uqyt9fjeR"

    headers = {
        "Host": "www.calendariopodismo.it",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/69.0.3497.81 Chrome/69.0.3497.81 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9,ca;q=0.8,es;q=0.7"
    }

    cookies = {
        "PHPSESSID": "2eaai0m8qqvcfnscirhgb72cl5",
        "_ga": "GA1.2.212921660.1537976895",
        "_gid": "GA1.2.471609515.1537976895",
        "displayCookieConsent": "y",
        "__atuvc": "15%7C39",
        "__atuvs": "5babaa3f29be5f3900e",
        "__atrfs": "ab/|pos/|tot/|rsi/5babaac200000000|cfc/|hash/0|rsiq/|fuid/df5f8de4|rxi/|rsc/|gen/1|csi/|dr/"
    }

    session = None
    csv_writer = None

    crawleds = list()
    _cached = dict()

    def __init__(self, **kwargs):
        self.url = kwargs.get("init_url", init_url)
        self.text = req.get(init_url).text
        self.selector = Selector(text=self.text)
        self.crawleds = list()
        self._cached = dict()
        self.setup_session()
        if kwargs["run"]:
            self.crawl(kwargs["file_path"])

    def setup_session(self):
        self.session = req.Session()

    def setup_writer(self, data):
        columns = list(data.keys())
        for key in ["riferimento", "luogo", "giorno", "data", "nome", "km", "telefoni", "note", "email", "web"]:
            if key not in columns:
                columns.append(key)
            
        if isfile(self.file_path):
            self.file = open(self.file_path, 'a')
            self.csv_writer = csv.DictWriter(self.file, fieldnames=columns)
        else:
            self.file = open(self.file_path, 'w')
            self.csv_writer = csv.DictWriter(self.file, fieldnames=columns)
            self.csv_writer.writeheader()

    def parse_crawled(self, data):
        self.crawleds.append(data)
        if self.csv_writer is not None:
            try:
                self.csv_writer.writerow(data)
            except:
                pass
        else:
            self.setup_writer(data)
            try:
                self.csv_writer.writerow(data)
            except:
                pass

        if len(self.crawleds) >= 100:
            self.file.close()
            self.csv_writer = None

    def request(self, url, **kwargs):
        try:
            request = req.Request("GET", url, headers=self.headers)
            prepped = self.session.prepare_request(request)

            res = self.session.send(prepped)
            if res.status_code in [400, 401, 403, 404, 405, 406, 407, 408, 500, 501, 502, 503, 504, 505, 506] and kwargs.get('count', 0) < 5:
                print('Retrying request after a handled http response code: ', res.status_code)
                success = self.request(url, count=(kwargs.get('count', 0)+1))

            elif res.status_code in [400, 401, 403, 404, 405, 406, 407, 408, 500, 501, 502, 503, 504, 505, 506] and kwargs.get('count', 0) >= 5:
                success = False
            else:
                print('Connection succes with status code: ', res.status_code)
                self.text = res.text
                self.selector = Selector(text=self.text)
                self.url = url

                success = True

        except ConnectionError as error:
            if kwargs.get("count", 0) >= 5:
                success = False
            else:
                print("\nError raised during connection to ", url)
                print(error)
                success = self.request(url, count=(kwargs.get("count", 0)+1))

        return success


    def follow(self, url, callback):
        if self._cached.get(url, None):
            return

        print('requesting: ', url)
        success = self.request(url)
        if success:
            callback()
            self._cached[url] = True
            sleep(0.5)

    def crawl(self, file_path):
        self.file_path = file_path
        if isfile(file_path):
            self._cached = {row[-2:][0]: True for row in csv.reader(open(self.file_path))}


        total_races = int(self.selector.xpath('//table[@id="content"]//td[@class="center"]/div/div//strong/text()')
                         .extract_first().replace(' manifestazioni', ''))

        page_offsets = [offset for offset in [i*999 for i in range(floor(total_races/999))] + [total_races]]
        print("detected a maximum number of pages of ", total_races)


        for offset in page_offsets:
            params = self.params.copy()
            params["startlimit"] = offset

            params = "&".join(["=".join([str(param[0]), str(param[1])]) for param in params.items()])
            url = "{!s}?{!s}#{!s}".format(self.base_url, params, self.hash)
            self.follow(url, self.crawl_races)

        print("amount of registereds crawleds: ", len(self.crawleds))

    def crawl_races(self):
        print("Start crawling races")
        p = re.compile('idrecord=(\w*)')
        race_ids = [p.search(url).groups()[0] for url in
                    self.selector.xpath('//tr[@class="rigacella"]/@onclick').extract()]

        print("crawled urls number: ", len(race_ids))
        for i in range(len(race_ids)):
            url = "{!s}?{!s}={!s}#{!s}".format(self.base_race_url, "idrecord", race_ids[i], self.hash)
            print(str(i))
            self.follow(url, self.parse_cursa)

    def parse_cursa(self):
        fields = [
            field.replace(':', '').lower() for field in
            self.selector.xpath('//table[3]//tr[2]//div/strong/text()').extract()
        ]

        values = [
            val.replace('\n', '').replace('\t', '').replace('\r', '').replace('  ', '') for val in
            self.selector.xpath('//table[3]//tr[2]//div/text()').extract()
        ][1:][:-1]


        new_fields, new_values = list(), list()
        for i in range(len(fields)):
            field = fields[i]
            if field != "email" and field != "web":
                new_fields.append(fields[i])
                new_values.append(values[i])

        emails = ", ".join([
            value for value in
            self.selector.xpath('//table[3]//tr[2]//div//a/text()').extract()
            if '@' in value
        ]).replace('mailto:', '')

        webs = ", ".join([
            value for value in
            self.selector.xpath('//table[3]//tr[2]//div//a/text()').extract()
            if 'www' in value or 'http' in value
        ]).replace('http://', '')

        new_fields.append("email")
        new_values.append(emails)

        new_fields.append("web")
        new_values.append(webs)

        values = new_values
        fields = new_fields

        cursa_info = {fields[i]: values[i] for i in range(len(fields))}
        cursa_info["url"] = self.url

        self.parse_crawled(cursa_info)

    def on_end(self, callback):
        self.end_spider = callback

    def end_spider(self, callback):
        return

    def __call__(self, *args, **kwargs):
        self.on_end(kwargs["on_end"])
        self.crawl(kwargs["file_path"])


if __name__ == "__main__":
    crawler = Crawler(init_url)
    file_path = sys.argv[1]
    print("output file defined as ", file_path)
    crawler.crawl(file_path)
