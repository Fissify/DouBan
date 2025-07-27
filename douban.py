# 加载版块
import requests
from bs4 import BeautifulSoup
import csv
import time
import random
from concurrent.futures import ThreadPoolExecutor
import threading
import math
from datetime import datetime

lock = threading.Lock()

# 排序方式名称映射
SORT_MODES = {
    'new_score': '推荐排序',
    'time': '时间排序'
}

# 推荐排序的三种类型
RECOMMENDATION_MODES = {
    'h': '好评',
    'm': '中评',
    'l': '差评'
}

def get_total_pages(movie_id, sort_mode, headers, percent_type=None):
    url = f'https://movie.douban.com/subject/{movie_id}/comments'
    params = {
        'sort': sort_mode,
        'status': 'P',
        'limit': 20
    }
    if percent_type:
        params['percent_type'] = percent_type

    response = requests.get(url, headers=headers, params=params, timeout=10)
    soup = BeautifulSoup(response.text, 'html.parser')

    # 查找评论总数的标签
    comment_summary = soup.find('li', class_='is-active').find('span')

    if comment_summary:
        total_str = comment_summary.text.strip()  # 例如'看过(137163)'
        print(f"提取的评论总数：'{total_str}'")  # 打印调试信息

        # 从字符串中提取数字部分（137163）
        digits = ''.join(filter(str.isdigit, total_str))
        if not digits:
            raise ValueError(f"评论总数字符串提取失败：'{total_str}'")

        total_num = int(digits)  # 将提取的数字转换为整数
        total_pages = min(math.ceil(total_num / 20), 25)  # 每页20条，最多25页
        return total_pages, total_num
    else:
        raise ValueError(f"页面未能成功获取评论数量，请检查页面结构或网络状况。")

def crawl_single_page(movie_id, page, sort_mode, headers, writer, percent_type=None):
    base_url = f'https://movie.douban.com/subject/{movie_id}/comments'
    params = {
        'start': page * 20,
        'limit': 20,
        'status': 'P',
        'sort': sort_mode
    }
    if percent_type:
        params['percent_type'] = percent_type

    try:
        print(f'[线程] {SORT_MODES.get(sort_mode, RECOMMENDATION_MODES.get(percent_type))} - 正在爬取第 {page + 1} 页...')
        res = requests.get(base_url, headers=headers, params=params, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        comment_items = soup.find_all('div', class_='comment')

        rows = []
        for item in comment_items:
            user = item.find('span', class_='comment-info').find('a').text.strip()
            rating_span = item.find('span', class_='rating')
            rating = rating_span['title'] if rating_span else '无评分'
            time_tag = item.find('span', class_='comment-time')
            time_str = time_tag['title'].strip() if time_tag else '未知时间'
            vote = item.find('span', class_='votes').text.strip()
            content = item.find('span', class_='short').text.strip()

            rows.append([SORT_MODES.get(sort_mode, RECOMMENDATION_MODES.get(percent_type)), user, rating, time_str, vote, content])

        # 使用锁来确保多线程时数据的正确写入
        with lock:
            for row in rows:
                writer.writerow(row)

        time.sleep(random.uniform(1, 2))

    except Exception as e:
        print(f'[错误] {SORT_MODES.get(sort_mode, RECOMMENDATION_MODES.get(percent_type))} - 第 {page + 1} 页出错：{e}')
        time.sleep(3)

def crawl_all_sort_modes(movie_id, headers):
    filename = f'douban_comments_{movie_id}_all_sorts.csv'
    with open(filename, mode='w', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file)
        writer.writerow(['排序方式', '用户名', '评分', '评论时间', '有用数', '评论内容'])

        # 先爬取推荐排序和时间排序
        for sort_mode in SORT_MODES:
            total_pages, total_comments = get_total_pages(movie_id, sort_mode, headers)
            print(f'\n【{SORT_MODES[sort_mode]}】共找到 {total_comments} 条评论，最多爬取 {total_pages} 页')

            # 使用线程池爬取每一页（线程设置为10，可自行修改）
            with ThreadPoolExecutor(max_workers=10) as executor:
                for page in range(total_pages):
                    executor.submit(crawl_single_page, movie_id, page, sort_mode, headers, writer)

        # 只爬取推荐排序中的好评、中评、差评
        for percent_type, mode_name in RECOMMENDATION_MODES.items():
            total_pages, total_comments = get_total_pages(movie_id, 'new_score', headers, percent_type)
            print(f'\n【{mode_name}】共找到 {total_comments} 条评论，最多爬取 {total_pages} 页')

            # 使用线程池爬取每一页（线程设置为10，可自行修改）
            with ThreadPoolExecutor(max_workers=10) as executor:
                for page in range(total_pages):
                    executor.submit(crawl_single_page, movie_id, page, 'new_score', headers, writer, percent_type)

    return filename

#主程序入口
if __name__ == '__main__':
    print("欢迎使用豆瓣电影评论爬虫（多排序版本）")
    movie_id = input('请输入电影id（如肖申克的救赎是1292052）: ').strip()

    headers = {
        'User-Agent': ',
        'Cookie':'
    }

    start_time = time.time()
    filename = crawl_all_sort_modes(movie_id, headers)
    end_time = time.time()

    now = datetime.now()
    print(f"\n爬取完成：{now.strftime('%Y-%m-%d')}，数据保存在 {filename}")
    print(f"总耗时：{end_time - start_time:.2f} 秒\n")

    input("所有数据爬取完成！按回车键关闭程序")
