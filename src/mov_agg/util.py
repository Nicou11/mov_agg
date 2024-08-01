import pandas as pd

def merge(load_dt="20240724"):
    read_df = pd.read_parquet('~/tmp/test_parquet')
    cols = ['movieCd', #영화의 대표코드를 출력합니다.
            'movieNm', #영화명(국문)을 출력합니다.
            #'openDt', #영화의 개봉일을 출력합니다.
            #'audiCnt', #해당일의 관객수를 출력합니다.
            'load_dt', # 입수일자
            'multiMovieYn', #다양성영화 유무
            'repNationCd', #한국외국영화 유무
            ]
    df = read_df[cols]
    df_w = df[(df['movieCd'] == '20247781') & (df['load_dt'] == int(load_dt))].copy()
    print(df_w)
    print(df_w.dtypes)

    # 카테고리 타입 -> Object
    df_w['load_dt'] = df_w['load_dt'].astype('object')
    df_w['multiMovieYn'] = df_w['multiMovieYn'].astype('object')
    df_w['repNationCd'] = df_w['repNationCd'].astype('object')
    print(df_w.dtypes)
    return df_w

merge()
