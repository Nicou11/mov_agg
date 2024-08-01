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
    #df_w['load_dt'] = df_w['load_dt'].astype('object')
    #df_w['multiMovieYn'] = df_w['multiMovieYn'].astype('object')
    #df_w['repNationCd'] = df_w['repNationCd'].astype('object')
    
    # NaN 값을 unknown 으로 변경
    #df_w['multiMovieYn'] = df_w['multiMovieYn'].fillna('unknown')
    #df_w['repNationCd'] = df_w['repNationCd'].fillna('unknown')
    #print(df_w.dtypes)
    #print(df_w)

    # 머지
    #u_mul = df_w[df_w['multiMovieYn'] == 'unknown']
    #u_nat = df_w[df_w['repNationCd'] == 'unknown']
    #m_df = pd.merge(u_mul, u_nat, on='movieCd', suffixes=('_m', '_n'))

    # 드랍
    df_multiMovieYn = df_w[df_w['multiMovieYn'].isnull()].drop(columns='multiMovieYn').reset_index(drop=True)
    df_repNationCd = df_w[df_w['repNationCd'].isnull()].drop(columns='repNationCd').reset_index(drop=True)
    df_merge = df_multiMovieYn.merge(df_repNationCd, on=['movieCd', 'movieNm', 'load_dt'], how='outer')
    
    print(df_merge)
    return df_merge


merge()
