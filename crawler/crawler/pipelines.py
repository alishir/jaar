# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import Boolean
from sqlalchemy import Integer
from sqlalchemy import DateTime
from datetime import datetime
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine

Base = declarative_base()


class DivarPost(Base):
    __tablename__ = "divar_home"

    id = Column(Integer, primary_key=True)
    token = Column(String(8))
    title = Column(String(1024))
    rahn_txt = Column(String(512))
    rent_txt = Column(String(512))
    post_date = Column(DateTime(timezone=True))
    last_post_date = Column(Integer)
    rahn = Column(Integer)
    rent = Column(Integer)
    desc = Column(String(4096))
    parking = Column(Boolean)
    elevator = Column(Boolean)
    cabinet = Column(Boolean)
    rooms_count = Column(Integer)
    space = Column(Integer)
    year = Column(Integer)
    commutable = Column(Boolean)
    family = Column(Boolean)
    single = Column(Boolean)
    personal_adv = Column(Boolean)
    post_page_raw = Column(String(10240))
    url = Column(String(512))


class CrawlerPipeline:

    def open_spider(self, spider):
        engine = create_engine("sqlite:///test.db", echo=False, future=True)
        Base.metadata.create_all(engine)
        self.session = Session(engine)

    def close_spider(self, spider):
        self.session.commit()
        self.session.close()

    def process_item(self, item, spider):
        last_post_date = item['last_post_date']
        post_date = datetime.utcfromtimestamp(last_post_date // 1000000)
        divar_post = DivarPost(
            title=item['title'], token=item['token'], rahn_txt=item['rahn_txt'],
            rent_txt=item['rent_txt'], last_post_date=last_post_date, post_date=post_date
        )

        try:
            self.session.add(divar_post)
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            spider.stop_on_duplicate = True
        return item
