import requests
import xlwt,xlrd
from collections import defaultdict
import time
import random
import pandas as pd
import os
from urllib.parse import urlencode
from tqdm import tqdm
from fake_useragent import UserAgent
import json
import pandas as pd
import logging
import argparse

# 添加cookies
cookies='MQCCSESSID=iuv33svcrqh98g9nadsqn7p523; acw_tc=968bf42416254878691263253e513e4d1178d0094fd77019e048093ea0;zg_de1d1a35bfa24ce29bbf2c7eb17e6c4f=%7B%22sid%22%3A%201625487537220%2C%22updated%22%3A%201625487799832%2C%22info%22%3A%201625487537223%2C%22superProperty%22%3A%20%22%7B%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDomain%22%3A%20%22%22%7D; zg_did=%7B%22did%22%3A%20%2217a769b743f0-0d9cff52eab0578-48183301-13c680-17a769b7440679%22%7D; UM_distinctid=17a769b6c84cb9-0196a52fc2f2da-48183301-13c680-17a769b6c8512a7;QCCSESSID=62ekemhrts34760d4aqg8q0ij4'
cookies=[i.strip() for i in cookies.split(';')]
cookies_list=[]
for c in cookies:
    cookies_dict={}
    key,value=c.split('=')[0],c.split('=')[1]
    cookies_dict[key]=value
    cookies_list.append(cookies_dict)
#加载多个proxy
ips=pd.read_json('./proxy_ip.json')
ips_list=[ips.data[i] for i in range(len(ips.data))]
all_proxies=[]
for ip_port in ips_list:
    signal_proxy = {"http": ''}
    signal_proxy["http"]='http://'+str(ip_port['ip'])+':'+str(ip_port['port'])
    all_proxies.append(signal_proxy)


'''
function:
    爬取企查查企业的地址信息，并且生成excel
args:
    path: 保存文件所在的路径
    res: 需要保存的excel名称
    logger: 日志handler
    delay: 每次请求直接的延时，注意延时方式是高斯延时，delay充当高斯的两个参数，当delay=0是没有延时
'''
class Compary_Address:
    def __init__(self,path,res,logger,delays=3):
        self.path=path
        self.res=res
        self.logger=logger
        self.delys=delays
        self.request_head() # request的header列表，用于后边随机分配每次请求的header
        self.read_excel()  # 读取需要查询的企业（是一个excel列表，不同任务可以修改）
        self.finshedSet = self.read_worked() # 读取已经爬取完的公司列表（模拟断点传输，把已经爬取完的保存在一个集合中，每次爬取完毕保存集合）

    # 设置请求头
    def request_head(self):
        self.ua = UserAgent()
        self.cookies_list=cookies_list
        self.all_proxies=all_proxies

    # 定义延时函数
    def delay_s(self):
        if self.delys==0:
            return time.sleep(0)
        else:
            return  time.sleep(random.gammavariate(self.delys,self.delys))

    # 读取已经爬取过的公司集合
    def read_worked(self,file='done.pickle'):
        if os.path.exists(os.path.join(path,file)):
            return pd.read_pickle(os.path.join(path,file))
        else: # 创建一个新的集合
            return set()

    # 读取需要爬取的列表
    def read_excel(self):
        file=os.path.join(self.path, 'target.xlsx')
        wb=xlrd.open_workbook(file)
        sheet=wb.sheets()[0]

        compary_col=sheet.col(0)
        market_col=sheet.col(1)
        provinces_col=sheet.col(2)
        compary_list,market_list,provinces_list=[],[],[]
        for i,value in enumerate(compary_col):
            if i==0: # 跳过列名
                continue
            compary_list.append(str(value).split('\'')[1])
            market_list.append(str(market_col[i]).split('\'')[1])
            provinces_list.append(str(provinces_col[i]).split('\'')[1])
        self.info={"compary":compary_list,"market":market_list,"provinces":provinces_list}

    # 爬取内容
    def getProvinceCode(self,url, compary):
        # 每次相同的请求设置相同的head
        self.headers = {"User-Agent": self.ua.random}
        self.cookies = random.choice(self.cookies_list)
        self.proixes = random.choice(self.all_proxies)

        logger.debug("resquest header:cookie={},header={},proxies={}".format(self.cookies,self.headers,signal_proxy))
        s = requests.session()
        # 第一次请求
        self.compary_url=self.deal_firstResponse(s, compary, url)
        # 第二次请求
        compary_address=self.deal_secondResponse(s,compary)

        return compary_address

    # 处理搜索页面爬取的内容
    def deal_firstResponse(self,s,compary,url):
        try:
            encode_res = urlencode({'k:': compary}, encoding='utf-8')
            encode_res = encode_res.split('=')[1]
            url = url + "web/search?key={}".format(encode_res)
            self.logger.debug('first resquest url:{}'.format(url))
            response = s.get(url, headers=self.headers, cookies=self.cookies, allow_redirects=False)
            self.first_statusCode=response.status_code
            if self.first_statusCode==200:
                response.encoding = response.apparent_encoding
                content = response.text
                # print(content)
                start = content.find('关注企业') + len('关注企业')
                end = content.find('扫一扫查看详情')
                mapStr = content[start:end]
                # print(mapStr)
                # 下一次请求之前延时
                self.delay_s()

                # 匹配查询到的第一个url
                index1 = mapStr.find('" href="') + len('" href="')
                index2 = mapStr.find('" class="title"')
                compary_url = mapStr[index1:index2]
                # print(self.compary_url)
            else:
                assert self.logger.error('stop request 1!')
        except:
            self.first_statusCode = -1
            self.logger.warning("{},请求1出错！！！".format(self.first_statusCode))
            return -2
        return compary_url

    # 处理第二次请求，匹配到的结果
    def deal_secondResponse(self,s,compary):
        if self.first_statusCode==200:
            try:
                self.logger.info('second request url:{}'.format(self.compary_url))
                print(self.compary_url)
                compary_response = s.get(self.compary_url, headers=self.headers, cookies=self.cookies, allow_redirects=False)
                compary_response.encoding = compary_response.apparent_encoding
                self.secode_statusCode=compary_response.status_code

                if self.secode_statusCode==200:
                    content = compary_response.text
                    # print(content)
                    # 下一次请求之前延时
                    s.keep_alive = False  # 关闭多余连接
                    self.delay_s()

                    # 开始处理查询到的结果
                    # 判断公司是不是对应
                    compary_name_response_start = content.find("<title>") + len("<title>")
                    compary_name_response_end = content.find("</title>")
                    compary_name_response = content[compary_name_response_start:compary_name_response_end]
                    compary_name_response = compary_name_response.split('-')[0].strip()

                    compary_address = None
                    # print(compary_name_response,compary)
                    if compary_name_response == compary:
                        # print("{} match successful!".format(compary_name_response))
                        # 查询地址
                        compary_address_start = content.find('地址：') + len('地址：')
                        compary_address_end = content.find('简介：')
                        compary_address_content = content[compary_address_start:compary_address_end]
                        # print(compary_address_content)
                        compary_address_index1 = compary_address_content.find('value="') + len('value="')
                        compary_address_index2 = compary_address_content.find('" class="copy_input"')
                        compary_address = compary_address_content[compary_address_index1:compary_address_index2]
                        # print(compary_address)

                        # 打印查询到的公司和地址
                        self.logger.info("需要查询的公司：{}\t查询到的公司：{}\t查询到的公司地址：{}".format(compary, compary_name_response, compary_address))
                        return compary_address
                else:
                    assert self.logger.error('{},stop request 2'.format(self.secode_statusCode))
            except:
                self.secode_statusCode = -1
                self.logger.warning("Not match {} !!!".format(compary))
                return -4

        else:
            return -3


    # 写入excel文件
    def write_excel(self):
        # 创建工作簿
        workbook = xlwt.Workbook(encoding='utf-8')
        # 创建sheet
        comparys = self.info["compary"]
        market = self.info["market"]
        provinces = self.info['provinces']

        comparys_results = defaultdict(str)

        logger.debug('have finished {} company:{}'.format(len(self.finshedSet),self.finshedSet))

        before=len(self.finshedSet)
        for compary in tqdm(comparys):
            # 查阅是否已经爬取成功，如果成功跳过，不重复爬取
            if compary not in self.finshedSet:
                compary_arrdess = self.getProvinceCode('https://www.qcc.com/', compary)
                logger.debug("status code:{},{}".format(self.first_statusCode,self.secode_statusCode))
                if self.first_statusCode==200 and self.secode_statusCode==200:
                    comparys_results[compary] = compary_arrdess
                    self.finshedSet.update([compary])
                else: # 终止程序，保存已经读取的
                    break

        comparys_results = dict(comparys_results)

        # 保存到excel里边
        data_sheet = workbook.add_sheet("奥铃服务站")
        # 设置列宽
        first_cow = data_sheet.col(0)
        seconde_cow = data_sheet.col(4)
        first_cow.width = 256 * 20 * 2
        seconde_cow.width = 256 * 20 * 2
        row0 = [u'服务站名称', u'市场部', u'省份', u'市', u'地址']  # 每个表的第一行文字，表头
        for i in range(len(row0)):
            data_sheet.write(0, i, row0[i])

        for index, item in enumerate(comparys_results.keys()):
            # 匹配爬取的公司
            indices=comparys.index(item)
            data_sheet.write(index + 1, 0, comparys[indices])
            data_sheet.write(index + 1, 1, market[indices])
            data_sheet.write(index + 1, 2, provinces[indices])

            if comparys_results[item]:
                # 加入市
                city_start = comparys_results[item].find('省') + len('省')
                if city_start < 0:
                    city_start = 0
                city_end = comparys_results[item].find('市') + len('市')
                city_name = comparys_results[item][city_start:city_end]
                data_sheet.write(index + 1, 3, city_name)
            data_sheet.write(index + 1, 4, comparys_results[item])

        # 保存爬取过的内容
        self.logger.debug("have finished:{}".format(self.finshedSet))
        end=len(self.finshedSet)
        pd.to_pickle(self.finshedSet,os.path.join(self.path,'done.pickle'))
        workbook.save(os.path.join(self.path, self.res))

        return len(self.finshedSet),before==end

"""创建logging"""
def getLogger(path):
    # 创建一个logger
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG) # log总开关等级
    # 创建hander，用于写入文件
    log_name=os.path.join(path,'run.log')
    log_file=logging.FileHandler(log_name,mode='w')
    log_file.setLevel(logging.DEBUG)# 输出到文件的总开关
    # 定义输出格式
    formater=logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    log_file.setFormatter(formater)
    # 将文件添加到hander中
    logger.addHandler(log_file)

    return logger

"""
function:
    查询单个公司的地址
args:
    path: 保存目录
    looger: 日志handler
    compary: 公司名称
"""
def singal_query(path,logger,compary):
    AddressQuery = Compary_Address(path, res=None, logger=logger,delays=0)
    address=AddressQuery.getProvinceCode('https://www.qcc.com/', compary)

    city_name=None
    if address:
        # 加入市
        city_start = address.find('省') + len('省')
        if city_start < 0:
            city_start = 0
        city_end = address.find('市') + len('市')
        city_name = address[city_start:city_end]
    return address,city_name

def agg_excel(file_list,path,logger, save_name):

    # 所有查询到的结果
    compary_list, market_list, provinces_list,city_list,address_list = [], [], [],[],[]
    # 查询失败的公司
    compary_QueryFailed=[]

    for file in file_list:
        file = os.path.join(path,'res'+str(file)+'.xls')
        wb = xlrd.open_workbook(file)
        sheet = wb.sheets()[0]

        compary_col = sheet.col(0)
        market_col = sheet.col(1)
        provinces_col = sheet.col(2)
        city_col=sheet.col(3)
        address_col=sheet.col(4)
        for i, value in enumerate(compary_col):
            if i == 0:  # 跳过列名
                continue
            compary_= str(value).split('\'')[1]
            compary_list.append(compary_)
            market_list.append(str(market_col[i]).split('\'')[1])
            provinces_list.append(str(provinces_col[i]).split('\'')[1])

            address_, city_=str(address_col[i]).split('\'')[1],str(city_col[i]).split('\'')[1]
            # 判断是否查询到的地址为空，保存匹配失败公司名称
            if not address_:
                print(compary_)
                logger.info("the {} address is None".format(compary_))
                compary_QueryFailed.append(compary_)

            address_list.append(address_)
            city_list.append(city_)
    print(len(compary_list),len(market_list),len(provinces_list),len(city_list),len(address_list))

    # 写入新的结果
    # 创建工作簿
    workbook = xlwt.Workbook(encoding='utf-8')
    # 保存到excel里边
    data_sheet = workbook.add_sheet("奥铃服务站")
    # 设置列宽
    first_cow = data_sheet.col(0)
    seconde_cow = data_sheet.col(4)
    first_cow.width = 256 * 20 * 2
    seconde_cow.width = 256 * 20 * 2
    row0 = [u'服务站名称', u'市场部', u'省份', u'市', u'地址']  # 每个表的第一行文字，表头
    for i in range(len(row0)):
        data_sheet.write(0, i, row0[i])

    for index in range(len(compary_list)):
        # 匹配爬取的公司
        data_sheet.write(index + 1, 0, compary_list[index])
        data_sheet.write(index + 1, 1, market_list[index])
        data_sheet.write(index + 1, 2, provinces_list[index])
        data_sheet.write(index + 1, 3, city_list[index])
        data_sheet.write(index + 1, 4, address_list[index])
    workbook.save(os.path.join(path, save_name))

    # 保存匹配失败的公司
    with open(os.path.join(path,'QueryFailed.txt'),'w+') as handle:
        for i in compary_QueryFailed:
            handle.write(i)
            handle.write('\n')


if __name__=='__main__':
    parser = argparse.ArgumentParser() # 创建一个解析对象
    parser.add_argument('--path',default='./',help='保存文件的路径')
    parser.add_argument('--indice',type=int,default=100,help='开始保存文件的目录，比如0，1，2')
    parser.add_argument('mode',type=int,help="程序执行的模式，mode=0,excel查询模式;model=1,单个公司查询;model=2,合并查询的结果")
    parser.add_argument('--file_list',help="合并查询结果时，需要聚合的文件列表")
    parser.add_argument('--save_name',default='test.xls',help="保存最后聚合的查询结果，excel名称,必须.xls结尾")

    args=parser.parse_args() #获取参数

    path=args.path
    # # 创建日志
    logger = getLogger(path)

    print(args.mode)
    # 查询单个公司
    if args.mode == 1:
        address, city = singal_query(path, logger, compary="百度")

    # 聚合查询的结果
    elif args.mode == 2:
        file_list = [i for i in args.file_list]
        if not file_list:
            assert print("请输入需要聚合的文件列表")
        save_name=args.save_name
        agg_excel(file_list, path, logger, save_name)

    # 查询excel
    elif args.mode==0:
        # 从断开的位置继续
        indice = 0
        logger.info("begin time={}，continue from {}".format(time.time(),indice))
        while True:
            res = 'res' + str(indice) + '.xls'
            project=Compary_Address(path,res,logger,delays=2)
            finshed_num,is_old=project.write_excel()

            # 防止生成空文件
            if not is_old:
                indice+=1

            if not finshed_num==len(project.info["compary"]):
                # 延时，等待恢复
                pass
                # time.sleep(10*60)
            else:
                break
        logger.info("end time is {}".format(time.time()))
    else:
        pass
