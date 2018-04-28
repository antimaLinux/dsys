from __future__ import print_function, unicode_literals
import scrapy
import os


class QuotesSpider(scrapy.Spider):
    name = "quotes"
    base_dir = "/tmp/"
    start_urls = [
        "https://www.sisal.it/win-for-life/estrazioni",
    ]

    def parse(self, response):
        page = response.url.split("/")[-2]
        filename = 'quotes-%s.html' % page
        with open(os.path.join(self.base_dir, filename), 'wb') as f:
            f.write(response.body)