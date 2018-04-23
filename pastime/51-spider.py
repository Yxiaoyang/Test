#coding=utf-8

import time, random, requests, queue, chardet, re, area, os, pymysql
from multiprocessing import Process, Pool
from threading import Thread
from bs4 import BeautifulSoup
from lxml import etree
from pypinyin import pinyin, lazy_pinyin


class JOB51:
    #定义初始属性
    def __init__(self, url, job, addr, table_name, partation, area_num, db_host, db_user, db_passwd, db_name):
        self.url = url
        self.job = job
        self.addr = addr
        self.addr_num = area_num
        self.user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36"
        self.refer = "www.51job.com"
        self.headers = {'UserAgent': self.user_agent, 'Referer': self.refer}
        self.table_name = table_name
        self.partation = partation
        self.db_host = db_host
        self.db_user = db_user
        self.db_passwd = db_passwd
        self.db_name = db_name
        self.q = queue.Queue()
        self.s = queue.Queue()
    #抓取职位链接线程
    def job_url(self):
        param = {
            'keyword': self.job,
            'jobarea': self.addr_num,
            'issuedate': 3,
            'curr_page': 1,
        }
        print(u'正在抓取%s的%s岗位链接信息，请稍等..' % (self.addr, self.job))
        while True:
            data = requests.get(self.url, params=param, headers=self.headers)
            data.encoding = chardet.detect(data.content)['encoding']
            html = BeautifulSoup(data.content, 'html.parser')
            job_links = html.select('a[onmousedown=""]')
            if bool(job_links) is False:
                print(u'最后一页抓取完成...')
                break
            for i in job_links:
                ihref = i.get('href')
                print(u"正在提交 %s:%s 到抓取等待队列." % (i.get('title'), ihref))
                self.q.put(ihref)
            param['curr_page'] += 1

    #抓取职位详情页线程
    def job_deatils(self):
        # self.job_url()
        while not self.q.empty():
            deatils_link = self.q.get()
            data = requests.get(deatils_link, headers=self.headers)
            data.encoding = chardet.detect(data.content)['encoding']
            html = etree.HTML(data.content)

            job_name = html.xpath('string(//h1[@title])')
            company_name = html.xpath('string(//p[@class="cname"]/a)')
            wage = html.xpath('string(//div[@class="cn"]/strong)')
            job_addr = html.xpath('string(//span[@class="lname"])')
            welfare = html.xpath('normalize-space(string(//p[@class="t2"]))')
            experience = html.xpath('string(//div[@class="t1"]/span)')
            education = html.xpath('string(//div[@class="t1"]/span[2])')
            job_content = html.xpath('normalize-space(string(//div[@class="bmsg job_msg inbox"]))')
            try:
                company_type = re.split('\|', html.xpath('normalize-space(string(//p[@class="msg ltype"]))'))[2].strip()
            except:
                company_type = 'null'
            job_url = deatils_link

            job_deatils = job_name,company_name,wage,job_addr,experience,education,job_content,welfare,company_type,job_url
            print(u'正在提交 %s 岗位详情到存储等待队列...' % job_name)
            self.s.put(job_deatils)

    # 新线程-存储数据到mysql
    def Mysql_Save(self):
        db = pymysql.connect(host=self.db_host, port=3306, user=self.db_user, passwd=self.db_passwd, db=self.db_name, charset='utf8')
        cursor = db.cursor()
        if not self.table_name:
            self.table_name = ''.join(lazy_pinyin(self.addr+"_"+job))
            sql_dr = "drop table if exists %s" % self.table_name
            cursor.execute(sql_dr)
        sql_cr = '''
        create table if not exists %s(job_name varchar(64),company_name varchar(64),wage varchar(32),job_addr varchar(64),experience char(32),education char(32),job_content text,welfare varchar(64),company_type varchar(64),job_url varchar(64));
        ''' % self.table_name
        cursor.execute(sql_cr)
        db.commit()
        count = 1
        while not self.s.empty():
            job_deatils = self.s.get()
            sql_in = '''insert into %s values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');
            ''' % (self.table_name, job_deatils[0], job_deatils[1], job_deatils[2], job_deatils[3], job_deatils[4], job_deatils[5], job_deatils[6], job_deatils[7], job_deatils[8], job_deatils[9])
            try:
                print(u'正在保存 %s 到数据表 %s ..' % (job_deatils[0], self.table_name))
                cursor.execute(sql_in)
                db.commit()
            except:
                db.rollback()

            # 调用本地磁盘存储方法
            if self.partation == "1":
                self.Partation_Save(job_deatils, count)
                count += 1

        db.close()

    # 存储数据到本地磁盘
    def Partation_Save(self, job_deatils, count):
        path = "d:\\51job\\"
        if not os.path.exists(path):
            os.mkdir(path)
        f = open(path+self.table_name+'.txt', "a+")
        f.write((u'''
%s.---------------------------------------------------------------------\n
%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n
        ''' % (count, job_deatils[0], job_deatils[1], job_deatils[2], job_deatils[3], job_deatils[4], job_deatils[5], job_deatils[6], job_deatils[7], job_deatils[8], job_deatils[9])).replace(u'\xa0', u''))
        f.close()

    # 线程启动
    def run(self):
        job_url = Thread(target=self.job_url())
        job_deatils = Thread(target=self.job_deatils())
        mysql_save = Thread(target=self.Mysql_Save())
        job_url.start()
        job_deatils.start()
        mysql_save.start()

# spider 入口,多进程启动
def app():
    global url, job, addr, table_name, partation, db_host, db_user, db_passwd, db_name
    pool = Pool(4)
    if addr:
        for i in range(len(addr.split(','))):
            i -= 1
            try:
                # run = JOB51(url, job, addr.split(',')[i], table_name, partation, area.area[addr.split(',')[i]], db_host, db_user, db_passwd, db_name)
                # pool.apply_async(run.run())
                # results = pool.map(JOB51(url, job, addr.split(',')[i], table_name, partation, area.area[addr.split(',')[i]], db_host, db_user, db_passwd, db_name).run())
                pool.apply_async(JOB51(url, job, addr.split(',')[i], table_name, partation, area.area[addr.split(',')[i]], db_host, db_user, db_passwd, db_name).run(),)
            except Exception as e:
                print(e)
    else:
        for addr, area_num in area.area.items():
            run = JOB51(url, job, addr, table_name, partation, area_num, db_host, db_user, db_passwd, db_name)
            pool.apply_async(run.run())
        pool.close()
        pool.join()



if __name__ == "__main__":
    print(u'''
    Hello,欢迎使用51job_spider
    1.岗位名称只能输入一个或者为空，为空表示所有岗位
    2.工作地点可以输入多个以英文逗号分开，如果为空表示抓取全国范围
    3.数据表名称可以自定义，如果为空默认表示 “地点_岗位”
    4.写入本地磁盘开关，1为写入，空或者其他数字为不写入
    5.数据库连接信息必须填写正确！填写的数据库需要提前创建好！！！
    6.test
    ''')
    url = "https://search.51job.com/jobsearch/search_result.php"
    job = input(u'请输入岗位名称：')
    addr = input(u'请输入工作地点：')
    table_name = input(u'请输入保存的数据表名称：')
    partation = input(u'请确认是否同时写入本地硬盘：')
    db_host = input(u'请输入数据库连接地址：')
    db_user = input(u'请输入数据库用户：')
    db_passwd = input(u'请输入数据库用户密码：')
    db_name = input(u'请输入已创建的数据库名称：')
    app()
    input(u'完毕！请输入任意键退出！')
    exit(0)


