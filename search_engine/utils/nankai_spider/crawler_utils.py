
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from nankai_spider.spiders.nankai import NankaiSpider

process = CrawlerProcess(get_project_settings())
process.crawl(NankaiSpider)
process.start()

# settings = get_project_settings()
# print(settings.attributes.keys())  # 看看settings有没有被加载