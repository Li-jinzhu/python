import datetime
import re
from queue import Queue
import threading
from pathlib import Path
from user_agents import parse
from collections import defaultdict
from collections import Counter

# logline 是数据格式， 这里留着作参考用
# logline = '95.108.181.8 - - [16/Jul/2017:06:34:08 +0800] "GET /job/index.php?c=search&page=1 HTTP/1.1" 200 8125 "-" "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)"'
#
pattern = '''(?P<remote>[\d\.]{7,}) - - \[(?P<datetime>[^\[\]]+)\] "(?P<request>[^"]+)" (?P<status>\d+) (?P<size>\d+) "(?P<referer>\w+://[\S]+)" "(?P<useragent>[^"]+)"'''
regex = re.compile(pattern)


def extract(line):
    matcher = regex.match(line)
    if matcher:
        return {k: ops.get(k, lambda x: x)(v) for k, v in matcher.groupdict().items()}

# 数据源
ops = {
    'datetime': lambda timestr: datetime.datetime.strptime(timestr, "%d/%b/%Y:%H:%M:%S %z"),
    'status': int,
    'size': int,
    'request': lambda request: dict(zip(('method', 'url', 'protocol'), request.split())),
    'useragent': lambda useragent: parse(useragent)
}

def openfile(path: str):
    with open(path) as f:
        for line in f:
            d = extract(line)
            if d:
                yield d
            else:
                # TODO 不合格数据有多少 解析失败就抛弃，或者 打印日志
                continue

def load(*path):
    """文件的装载"""
    for file in path:
        p = Path(file)
        if not p.exists():
            continue

        if p.is_dir():
            for x in p.iterdir():
                if x.is_file():
                    yield from openfile(str(x))
        elif p.is_file():
            yield from openfile(str(p))
            """
            yield from openfile(str(p)) ==
            for x in openfile(str(p)):
                yield x
            """

############################################################################################
def window(src: Queue, handler, width: int, interval: int):
    """
    窗口函数
    :param src: 数据源，生成器，用来拿数据
    :param handler: 数据处理函数
    :param width: 时间窗口宽度， 秒
    :param interval: 处理时间间隔， 秒
    :return:
    """
    start = datetime.datetime.strptime('1970/01/01 01:01:01 +0800', '%Y/%m/%d %H:%M:%S %z')
    current = datetime.datetime.strptime('1970/01/01 01:01:01 +0800', '%Y/%m/%d %H:%M:%S %z')
    delta = datetime.timedelta(seconds=width - interval)

    buffer = []

    while True:
        data = src.get()# block 阻塞

        if data:
            buffer.append(data)
            current = data['datetime']

        if (current - start).total_seconds() >= interval:
            ret = handler(buffer)
            print(ret)

            start = current
            # buffer 的处理  重叠方案
            buffer = [i for i in buffer if i['datetime'] > current - delta]

############################################################################################
# 处理函数
def handler(iterable: list):
    vals = [x['value'] for x in iterable]
    return sum(vals) // len(vals)

# 测试
def donothing_handler(iterable: list):
    print(iterable)
    return iterable

# 状态码分析
def status_handler(iterable: list):
    d = {}
    for item in iterable:
        key = item['status']
        if key not in d.keys():
            d[key] = 0
        d[key] += 1

    total = sum(d.values())
    return {k: v/total*100 for k, v in d.items()}

# 浏览器分析
ua_dict = defaultdict(lambda : 0)
def brower_handler(iterable: list):
    for item in iterable:
        ua = item['useragent']
        # ua.browser.family, ua.browser.version_string
        key = (ua.browser.family, ua.browser.version_string)
        ua_dict[key] += 1
    return ua_dict

# IP地址统计
remote_dict = defaultdict(lambda : 0)
def remote_handler(iterable: list):
    for item in iterable:
        key = item['remote']
        remote_dict[key] += 1
    return Counter(remote_dict).most_common()

#############################################################################################
# 分发器
def dispatcher(src):
    # 队列列表
    queques = []
    threads = []

    def reg(handler, width, interval):
        """
        注册窗口处理函数
        :param handler: 注册的数据处理函数
        :param width:  时间窗口宽度
        :param interval: 时间间隔
        """
        q = Queue()
        queques.append(q)

        # 线程
        t = threading.Thread(target=window, args=(q, handler, width, interval))
        threads.append(t)

    def run():
        for t in threads:
            t.start()# 启动线程

        for x in src:
            for q in queques:
                q.put(x)

    return reg, run

################################################################################################
if __name__ == "__main__":
    import sys
    # path = sys.argv[1]
    path = 'test.log'

    reg, run = dispatcher(load(path))
    # reg注册 窗口
    reg(remote_handler, 5, 5)
    #reg(brower_handler, 5, 5)
    #reg(status_handler, 1, 2)

    # 启动
    run()
