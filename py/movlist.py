import os
import json
import time
from tqdm import tqdm
import requests

API_KEY = os.getenv('MOVIE_API_KEY')

def save_json(data, file_path):
    # 파일 저장 경로 생성 (이미 존재해도 무관)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # 기존 데이터가 있다면 불러오기
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
    else:
        existing_data = []

    # 새로운 데이터를 기존 데이터에 추가
    existing_data.extend(data)

    # JSON 파일에 데이터 저장
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, indent=4, ensure_ascii=False)

def req(url):
    r = requests.get(url).json()
    return r

def save_movies(year, per_page=10, sleep_time=1):
    # 하나의 movieList.json 파일에 모든 데이터를 저장
    file_path = os.path.expanduser('~/data/movbotdata/movieList.json')

    url_base = f"https://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={API_KEY}&openStartDt={year}&openEndDt={year}"

    print(f"{year}년 영화정보를 불러옵니다.")

    r = req(url_base + "&curPage=1")
    tot_cnt = r['movieListResult']['totCnt']
    #total_pages = (tot_cnt // per_page) + 1
    total_pages=10
    all_data = []
    for page in tqdm(range(1, total_pages + 1)):
        time.sleep(sleep_time)
        r = req(url_base + f"&curPage={page}")
        d = r['movieListResult']['movieList']
        all_data.extend(d)

    # 기존 데이터와 합쳐서 저장
    save_json(all_data, file_path)
    return True

if __name__ == "__main__":
    import sys
    year = sys.argv[1]  # 첫 번째 인수를 연도로 사용
    save_movies(year)  # save_movies 함수 호출

