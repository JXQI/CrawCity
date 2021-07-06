# CrawCity

**实现功能：**<br>从企查查网站爬取一些公司的地址信息，并且保存结果到excel中去<br><br>
**不同方式：**<br>
1.查询单个公司地址<br>
2.查询多个公司地址(excel列表)<br>3.聚合查询的结果(因为会被封ip,所以分多次请求，保存每一次请求的结果)<br><br>

**需要安装的库:**<br>

xlrd==1.2.0<br>
xlwt==1.3.0<br>
fake-useragent==0.1.11<br>
requests==2.25.0<br><br>

**文件说明：<br>**
main.py 主程序<br>
proxy_ip.json 代理ip，json格式(可选，目前代码没有使用)<br>
done.pickle 已经完成的公司名（用于一次查询多个公司，每次ip被封之后保存结果，类似于断点传输）<br>
res0.xls等 每次查询的结果（由于ip会被封，因为每个公司列表可能需要分多次查询）<br>
run.log  运行程序的日志<br>
target.xlsx 需要查询的公司名称<br>
QueryFailed.txt 查询失败的公司名称<br>
final_result2.xls  查询的结果（名字可以自己配置）<br><br>

**运行说明：<br>**
`usage: main.py [-h] [--path PATH] [--indice INDICE] [--file_list FILE_LIST] [--save_name SAVE_NAME] mode
`<br>
`python main.py 2 --file_list=01234`<br>
其中mode是每次运行程序的模式：0代表查询多个公司(excel),1代表查询单个公司,2代表聚合查询的结果<br>
<br>
需要注意的是：<br>1.model=0 时需要注意indice的值，从已经保存的文件序号开始，不用每次都从0开始<br>
2.mode=2 时，需要输入file_list用来聚合最后的结果<br>
3.第一次运行注意删掉res0.xls等还有done.pickle文件，不然可能无法生成最新的结果<br><br>

**存在问题：<br>**
1.需要查询的公司名称变更的情况无法查询到<br>
2.代理ip用起来还是会被封掉<br><br>  

TODO:<br>
1.补充https/http的参数含义<br>
2.用seleuim完成，比较和request的区别<br>
3.尝试用数据库代替excel保存结果<br>









