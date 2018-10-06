import requests as req
from scrapy.selector import Selector
from datetime import datetime as dt
import csv
import sys
from time import sleep
from os.path import isfile
import re

year = dt.now().year
month = dt.now().month
dates = ['{!s}-{!s}'.format(year, (str(month + i) if len(str(month+i)) == 2 else '0' + str(month + i))) for i in list(range(13-month))] \
        + ['{!s}-{!s}'.format(year+1, str(i) if len(str(i)) == 2 else '0' + str(i)) for i in list(range(12-(12-month))) if i > 0]

start_date = dates[0]
init_url = 'https://runedia.mundodeportivo.com/calendario-carreras'


class Crawler:

    headers = {
        'Host': 'runedia.mundodeportivo.com',
        'User-Agent': 'Mozilla/5.0(X11;Ubuntu;Linuxx86_64;rv',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip,deflate,br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }

    cookies = {
        'language': 'es',
        ' eupubconsent': 'BOUWnLqOUWnLqAKAIAENAAAA-AAAAA',
        ' euconsent': 'BOUWnLqOUWnLqAKAIBENBn-AAAAhd7_______9______9uz_Gv_v_f__33e8__9v_l_7_-___u_-33d4-_1vX99yfm1-7ftr3tp_86ues2_Xur_9p93snDA',
        ' PHPSESSID': 'ej03em642ciqpvav0afs70ici5',
        ' _cmpQcif3pcsupported': '1',
        ' _ga': 'GA1.2.327062924.1537385751',
        ' _gid': 'GA1.2.1616989921.1537908449',
        ' _ga_ru': 'GA1.2.45015492.1536583310',
        ' _ga_ru_gid': 'GA1.2.1496165027.1537908449',
        ' _ga_ru_md': 'GA1.2.45333469.1536583310',
        ' _ga_ru_md_gid': 'GA1.2.296984180.1537908449'
    }

    session = None

    csv_writer = None


    def __init__(self, **kwargs):
        self.url = kwargs.get("init_url", init_url)
        self.text = req.get(self.url).text
        self.selector = Selector(text=self.text)
        self.crawleds = list()
        self._cached = dict()
        self.setup_session()

        if kwargs["run"]:
            self.crawl(kwargs["file_path"])


    def setup_writer(self, data):
        columns = list(data.keys())
        for key in ["country", "ccaa", "province", "type", "distance", "url", "telephone", "date", "name", "status",
                    "web", "mail"]:
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

    def setup_session(self):
        self.session = req.Session()

    def request(self, url, **kwargs):

        try:
            request = req.Request("GET", url, headers=self.headers)
            prepped = self.session.prepare_request(request)

            res = self.session.send(prepped)
            if res.status_code in [400, 401, 403, 404, 405, 406, 407, 408, 500, 501, 502, 503, 504, 505,
                                   506] and kwargs.get('count', 0) < 5:
                print('Retrying request after a handled http response code: ', res.status_code)
                success = self.request(url, count=(kwargs.get('count', 0) + 1))

            elif res.status_code in [400, 401, 403, 404, 405, 406, 407, 408, 500, 501, 502, 503, 504, 505,
                                     506] and kwargs.get('count', 0) >= 5:
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
                success = self.request(url, count=(kwargs.get("count", 0) + 1))

        return success

    def follow(self, url, callback, **data):
        if self._cached.get(url, None):
            return

        print('requesting: ', url)
        success = self.request(url)
        if success:
            callback(data)
            self._cached[url] = True
            sleep(0.5)

    def crawl(self, file_path):
        self.file_path = file_path
        if isfile(file_path):
            self._cached = {row[11]: True for row in csv.reader(open(file_path))}

        paises = [option for option in self.selector.xpath('//select[@id="pais"]//option/@value').extract() if option != "0"]
        min_distance = [dist for dist in self.selector.xpath('//select[@id="distmin"]//option/@value').extract() if dist != "1_5"]
        max_distance = ["0"] + [dist for dist in self.selector.xpath('//select[@id="distmax"]//option/@value').extract() if dist != "1_5"]
        distances = ['{!s}-{!s}'.format(min_distance[i], max_distance[i + 1]) for i in range(len(min_distance))]

        types = [type for type in self.selector.xpath('//select[@id="tipus"]//option/@value').extract() if "global" in type]

        for country in paises:
            print('request: ', 'https://runedia.mundodeportivo.com/funcs/f-cursa-selects-area.php?funcion=ch_pais&pais={!s}'
                        .format(country))
            r = req.get('https://runedia.mundodeportivo.com/funcs/f-cursa-selects-area.php?funcion=ch_pais&pais={!s}'
                        .format(country))
            print("request status code: ", r.status_code)

            ccaa = [
                ca for ca in Selector(text=r.text).xpath('//option/@value').extract()
                if ca != "0" and ca != ""
                and ca not in ['andalucia', 'aragon', 'asturias', 'canarias', 'cantabria', 'castilla-la-mancha', 'castilla-y-leon']
            ]

            for ca in ccaa:
                print('request: ', 'https://runedia.mundodeportivo.com/funcs/f-cursa-selects-area.php?funcion=ch_ccaa&ccaa={!s}'
                            .format(ca))
                r = req.get('https://runedia.mundodeportivo.com/funcs/f-cursa-selects-area.php?funcion=ch_ccaa&ccaa={!s}'
                            .format(ca))
                print("request status code: ", r.status_code)

                provinces = [
                    prov for prov in Selector(text=r.text).xpath('//option/@value').extract()
                    if prov != "0" and prov != ""
                       and prov not in ['andorra', 'barcelona', 'girona', 'lleida']
                ]

                for province in provinces:
                    for type in types:
                        for distance in distances:
                            for date in dates:
                                data = dict(
                                    country=country,
                                    ccaa=ca,
                                    province=province,
                                    type=type
                                )

                                res = req.post('https://runedia.mundodeportivo.com/controllers/curses.index.php', data=dict(
                                    accio="donaUrlFiltres",
                                    date=date,
                                    tipo=type,
                                    distancia=distance+"km",
                                    distMin=distance.split('-')[0].replace(' ', ''),
                                    distMax=distance.split('-')[1].replace(' ', ''),
                                    pais=country,
                                    ccaa=ca,
                                    provincia=province,
                                    nomesAmbInscripcio=False,
                                    pag=0,
                                    idioma="es"
                                ), headers={
                                    "Host": "runedia.mundodeportivo.com",
                                    "Connection": "keep-alive",
                                    "Content-Length": "166",
                                    "Accept": "application/json,text/javascript,*/*;q=0.01",
                                    "X-NewRelic-ID": "VQcHUVVbARABVVNTBAQAUw==",
                                    "Origin": "https",
                                    "X-Requested-With": "XMLHttpRequest",
                                    "User-Agent": "Mozilla/5.0(X11;Linuxx86_64)AppleWebKit/537.36(KHTML,likeGecko)UbuntuChromium/69.0.3497.81Chrome/69.0.3497.81Safari/537.36",
                                    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                                    "Referer": "https",
                                    "Accept-Encoding": "gzip,deflate,br",
                                    "Accept-Language": "en-US,en;q=0.9,ca;q=0.8,es;q=0.7"
                                })

                                if res.json()["success"] == "ok":
                                    self.follow(res.json()["url"], self.crawl_races, data=data)


        print("amount of registereds crawleds: ", len(self.crawleds))

    def crawl_races(self, kwargs):
        print("Start crawling races")
        urls = self.selector.xpath('//div[@class="colTwo"]/div[@class="cal-cont-cursa-fila-item-title"]/a/@href').extract()
        titles = self.selector.xpath('//div[@class="colTwo"]/div[@class="cal-cont-cursa-fila-item-title"]/a/text()').extract()
        distances = [
            dist.replace(' ', '').replace('\n', '').replace('\r', '') for dist in
            self.selector.xpath('//div[@class="colThree"]//div[@class="cal-cont-cursa-fila-item-dist1"]/text()').extract()
        ]

        print("crawled urls number: ", len(urls))
        for i in range(len(titles)):
            race_data = kwargs["data"].copy()
            race_data.update({"name": titles[i], "distance": distances[i]})
            print(str(i)+' / '+str(len(urls)))
            self.follow(urls[i], self.parse_cursa, data=race_data)

    def parse_cursa(self, kwargs):
        # content = self.selector.xpath('//div[@id="content"]')
        # date_container = self.selector.xpath('//div[@class="race_date_container"]')

        telRe = re.compile(r'^[0-9]{7,12}$')
        cursaInfo = dict(
            status=" ".join([
                span.replace('  ', '').replace('\n', '').replace('\r', '')
                for span in self.selector.xpath('//div[contains(@class,"race_status")]/text()').extract()
                if len(span) > 0
            ]),
            date=self.selector.xpath('//p//strong/text()').extract_first(),
            web=", ".join([
                url for url in
                self.selector.xpath('//span[contains(@class,"race_contact")]//a/@href').extract()
                if "http" in url
            ]).replace('http://', ''),
            mail=", ".join([
                mail for mail in
                self.selector.xpath('//span[contains(@class,"race_contact")]//a[contains(@href, "mailto")]/@href').extract()
                if "mailto" in mail
            ]).replace("mailto:", ""),
            telephone=", ".join([
                tel for tel in
                self.selector.xpath('//div[contains(@class, "race_info_row")]/span[contains(@class,"t_orange")]/text()').extract()
                if telRe.search(tel)
            ]),
            url=self.url
        )

        kwargs["data"].update(cursaInfo)

        self.parse_crawled(kwargs["data"])

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
