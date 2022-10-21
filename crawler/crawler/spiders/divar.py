# coding: utf-8

import scrapy
from scrapy.exceptions import CloseSpider
import json
import time
from parsivar import Normalizer
from parsivar import Tokenizer


class DivarSpider(scrapy.Spider):
    name = 'divar'
    allowed_domains = ['divar.ir']
    list_base_url = 'https://api.divar.ir/v8/web-search/4/residential-rent'
    post_base_url = 'https://divar.ir/v/'
    json_schema = {
        "category": {
            "value": "residential-rent"
        },
        "cities": [
            "4"
        ]
    }
    headers = {
        "sec-ch-ua": '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
        "Accept": 'application/json, text/plain, */*',
        "Content-Type": "application/json",
        "Referer": "https://divar.ir/",
        "sec-ch-ua-mobile": "?0",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        "sec-ch-ua-platform": "Linux"
    }
    # Control crawler from pipeline
    stop_on_duplicate = False

    def start_requests(self):
        body = {
            "json_schema": self.json_schema,
            "last-post-date": time.time_ns() // 1000
        }
        self.normalizer = Normalizer(statistical_space_correction=True)
        self.tokenizer = Tokenizer()
        yield scrapy.Request(method='POST', url=self.list_base_url,
                             callback=self.parse_list, headers=self.headers,
                             body=json.dumps(body))

    def parse_post(self, response):
        if self.stop_on_duplicate:
            raise CloseSpider('Already been scraped.')
        title_xpath = '/html/body/div/div[1]/div/div/div[1]/div[1]/div/div[1]/text()'
        info_xpath = '/html/body/div/div[1]/div/div/div[1]/div[4]'
        desc_xpath = '/html/body/div/div[1]/div/div/div[1]/div[5]/div[2]/div/p/text()'

        info_sel = response.xpath(info_xpath)
        info = self.extract_info(info_sel)

        title = response.xpath(title_xpath).get()
        title = self.normalizer.normalize(title)

        desc = response.xpath(desc_xpath).get()
        desc = self.normalizer.normalize(desc)

        yield {
            'id': response.meta['token'],
            'url': response.url,
            'title': title,
            'desc': desc,
            'year': info['year'],
            'space': info['space'],
            'rahn': info['rahn'],
            'rent': info['rent'],
            'elevator': info.get('elevator', False),
            'parking': info.get('parking', False),
            'cabinet': info.get('cabinet', False)
        }

    def parse_list(self, response):
        body = json.loads(response.body)
        posts = body["web_widgets"]["post_list"]

        for post in posts:
            data = post["data"]
            token = data["token"]
            url = self.post_base_url + token
            yield scrapy.Request(method='GET', url=url, meta={'token': token},
                                 callback=self.parse_post, headers=self.headers)

        last_post_date = posts[0]['action_log']['server_side_info']['info']['extra_data']['last_post_sort_date']
        day_epocs_us = 86400 * 1000 * 1000
        last_date = (time.time_ns() // 1000) - (2 * 30 * day_epocs_us)
        if last_post_date > last_date:
            body = {
                "json_schema": self.json_schema,
                "last-post-date": last_post_date
            }
            yield scrapy.Request(method='POST', url=self.list_base_url,
                                 callback=self.parse_list, headers=self.headers,
                                 body=json.dumps(body))

    def extract_info(self, info_selector):
        info = {}

        info_rows = info_selector.css('.kt-group-row-item--info-row')
        for r in info_rows:
            key_found = False
            title = r.xpath('./span[1]/text()').get()
            value = r.xpath('./span[2]/text()').get()

            title = self.normalizer.normalize(title)
            value = self.normalizer.normalize(value)

            if title == 'متراژ':
                key = 'space'
                key_found = True
            elif title == 'اتاق':
                key = 'rooms'
                key_found = True
            elif title == 'ساخت':
                key = 'year'
                key_found = True
            elif 'ودیعه' in title:
                key = 'rahn'
                value = self.tokenize_price(value)
                key_found = True
            elif 'اجار' in title:
                key = 'rent'
                value = self.tokenize_price(value)
                key_found = True

            if key_found:
                info[key] = value

        featurs = info_selector.css(
            '.kt-group-row-item__value.kt-body.kt-body--stable')
        for f in featurs:
            txt = f.xpath('./text()').get()
            fn = self.normalizer.normalize(txt)
            if fn == 'انباری':
                key = 'cabinet'
                value = True
            elif fn == 'انباری ندارد':
                key = 'cabinet'
                value = False
            elif fn == 'پارکینگ':
                key = 'parking'
                value = True
            elif fn == 'پارکینگ ندارد':
                key = 'parking'
                value = False
            elif fn == 'آسانسور':
                key = 'elevator'
                value = True
            elif fn == 'آسانسور ندارد':
                key = 'elevator'
                value = False

            info[key] = value

        vinfo_rows = info_selector.css(
            '.kt-base-row.kt-base-row--large.kt-unexpandable-row')
        for r in vinfo_rows:
            key_found = False
            title = r.xpath('./div[1]/p/text()').get()
            value = r.xpath('./div[2]/p/text()').get()

            title = self.normalizer.normalize(title)

            if title == 'ودیعه':
                key = 'rahn'
                key_found = True
                value = self.tokenize_price(value)
            elif 'ماهانه' in title:
                key = 'rent'
                value = self.tokenize_price(value)
                key_found = True

            if key_found:
                info[key] = value

        return info

    def tokenize_price(self, value):
        parts = self.tokenizer.tokenize_words(value)
        if len(parts) == 1:
            return 0
        elif len(parts) == 2:
            value = self.normalizer.normalize(parts[0])
            value = ''.join(filter(str.isdigit, value))
            value = int(value) / 1000000.0
            return value
