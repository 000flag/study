import pymysql
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dateutil import parser
import json
import os

# 데이터베이스 연결
def get_connection():
    host = input("DB 호스트: ")
    user = input("DB 사용자명: ")
    password = input("DB 비밀번호: ")
    db = input("DB 이름: ")
    port = int(input("포트 (기본 3306): ") or "3306")

    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=db,
        port=port,
        charset='utf8'
    )
    print(f"\n>>> DB 연결 성공: {db}\n")
    return conn

# 테이블 목록
def get_table_list(cursor, db_name):
    cursor.execute(f"SHOW TABLES FROM `{db_name}`")
    return [row[0] for row in cursor.fetchall()]

# 데이터 타입이 datetime 또는 timestamp인 컬럼명만 추출
def get_datetime_column(cursor, db_name, table):
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """, (db_name, table))

    # datetime_columns는 데이터 타입이 datetime 또는 timestamp인 컬럼명만 추출한 리스트
    datetime_columns = [row[0] for row in cursor.fetchall()
                        if row[1].lower() in ("datetime", "timestamp")]

    '''
        not datetime_columns는 리스트가 비어 있으면 True        
        즉, datetime 컬럼이 하나도 없으면,
        → None(None은 "값이 없다", "비어 있다", "아직 정의되지 않았다"는 의미를 갖는 특별한 객체)을 리턴해서 해당 테이블은 건너뛰게 만드는 코드
    '''
    if not datetime_columns:
        return None

    # datetime 또는 timestamp 타입의 컬럼들 중에서, 정해진 우선순위(priority)에 따라 가장 먼저 등장하는 컬럼명을 선택해서 반환하는 로직
    priority = ['reg_dt', 'created_at', 'insert_dt', 'log_time'] # 기준 날짜 컬럼으로 선호하는 컬럼 이름들을 순서대로 정의한 리스트
    for col in priority: # 정의한 우선순위 리스트를 순회
        if col in datetime_columns: # 현재 테이블에 존재하는 datetime_columns 리스트(= datetime/timestamp 컬럼들) 안에 지금 우선순위 컬럼이 있는지 확인
            return col # 발견되면 즉시 그 컬럼 이름을 반환하고 함수 종료

    return datetime_columns[0] #  datetime 컬럼 중 첫 번째 항목을 fallback(원하는 조건이 충족되지 않을 때 사용할 '대안')으로 사용 즉, 원하는 우선 컬럼이 없으면 첫 번째 값을 사용

'''
    value가 비정상적인 날짜인지 체크하고, 아니면 True를 반환하는 함수
    다음 값들을 '유효하지 않다'고 판단
        - "0000-00-00"
        - "0000-00-00 00:00:00"
        - None
        - ""
'''
def is_valid_datetime(value):
    return value not in ("0000-00-00", "0000-00-00 00:00:00", None, "")

'''
    MySQL 테이블에서 특정 datetime 컬럼의 "최소값(가장 오래된 날짜)"과 "최대값(가장 최신 날짜)"을 구해서 반환하는 함수

    cursor: 쿼리를 실행할 MySQL 커서 객체
    db_name: 조회할 데이터베이스 이름
    table: 조회할 테이블 이름
    datetime_col: 조회 기준이 되는 datetime 또는 timestamp 컬럼명
'''
def get_date_range(cursor, db_name, table, datetime_col):
    try:
        # 해당 테이블에서 IS NOT NULL 조건을 추가하여 NULL 값은 제외하고 datetime 컬럼 값 중 가장 오래된 날짜(MIN), 가장 최신 날짜(MAX) 를 가져오기
        cursor.execute(f"""
            SELECT MIN(`{datetime_col}`), MAX(`{datetime_col}`)
            FROM `{db_name}`.`{table}`
            WHERE `{datetime_col}` IS NOT NULL
        """)

        # cursor.fetchone(): SQL 쿼리 결과에서 딱 한 줄(row)만 가져오는 함수
        result = cursor.fetchone()
        print(f" fetchone() 결과: {result}")

        '''
            result 자체가 비어있거나 가져온 최소/최대값이 "0000-00-00" 같은 비정상 값일 경우
            → (None, None)을 반환해서 해당 테이블은 날짜 분석 불가로 처리
        '''
        if not result or not is_valid_datetime(result[0]) or not is_valid_datetime(result[1]):
            return None, None

        # 문자열로 반환된 날짜라면 → parser.parse()를 이용해 datetime 객체로 변환, datetime 객체면 그대로 사용
        start = parser.parse(result[0]) if isinstance(result[0], str) else result[0]
        end = parser.parse(result[1]) if isinstance(result[1], str) else result[1]
        print(f" 시작일: {start}, 종료일: {end}")

        return start, end # 시작 날짜(start), 종료 날짜(end)를 반환

    except Exception as e:
        print(f"쿼리 실패: {table} - {e}")
        return None, None

# 특정 테이블의 "평균 한 행(row)이 차지하는 바이트 크기(Byte)"를 조회하기 위한 함수
def get_avg_row_length(cursor, db_name, table):
    '''
        information_schema.tables: MySQL에서 모든 테이블의 메타데이터(정보)가 저장된 내부 테이블
        AVG_ROW_LENGTH: 이 필드는 해당 테이블에서 평균적으로 한 행이 얼마나 많은 바이트를 차지하는지를 의미
        WHERE table_schema = %s AND table_name = %s: 지정한 데이터베이스(db_name)와 테이블(table)에 대해 쿼리를 실행
    '''
    cursor.execute("""
        SELECT AVG_ROW_LENGTH
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    """, (db_name, table))
    result = cursor.fetchone()
    print(f"[DEBUG] {table} 테이블의 AVG_ROW_LENGTH 결과: {result}")

    '''
        result[0]: AVG_ROW_LENGTH 값
        이 값이 존재하지 않거나 0이면 0을 리턴
    '''
    return result[0] if result and result[0] else 0

'''
    주어진 시작일(start)과 종료일(end) 사이를 원하는 단위(unit)로 나눠서, 각 구간에 대한 라벨과 날짜 범위를 리스트로 반환하는 함수
    특정 기간을 "주 단위 / 월 단위 / 연 단위"로 나누기 위해 사용
    예: "2023년 1월 ~ 2023년 3월까지 월별로 얼마나 데이터가 있지?" 등을 분석할 때 사용

    start:	시작 날짜 (예: 2023-01-01)
    end:	끝 날짜 (예: 2023-12-31)
    unit:	'week', 'month', 'year' 중 하나를 전달
'''
def date_range_by_unit(start, end, unit):
    ranges = []
    current = start

    while current < end: # 종료일 도달 전까지 반복
        if unit == 'week':
            next_point = current + timedelta(days=7)
            label = f"{current:%Y-W%U}" # 주차 포맷
        elif unit == 'month':
            next_point = current + relativedelta(months=1)
            label = f"{current:%Y-%m}"
        elif unit == 'year':
            next_point = current + relativedelta(years=1)
            label = f"{current:%Y}"
        else:
            raise ValueError("단위 오류")

        ranges.append((label, current, next_point)) # 구간 저장
        current = next_point # 다음 구간 시작일로 이동

    return ranges

'''
    주어진 날짜 범위(start ~ end) 내에 들어 있는 행의 수를 기반으로 데이터 용량(MB)을 추정하는 함수

    cursor: DB 커서
    db_name: DB 이름
    table: 테이블 이름
    datetime_col: 날짜 기준 컬럼
    start, end: 날짜 범위
    avg_len: 한 행의 평균 길이 (bytes)
'''
def estimate_storage(cursor, db_name, table, datetime_col, start, end, avg_len):
    # 해당 구간의 행(row) 개수를 가져옴 → row_count
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM `{db_name}`.`{table}`
        WHERE `{datetime_col}` >= %s AND `{datetime_col}` < %s
    """, (start, end))

    row_count = cursor.fetchone()[0] # 한 행의 첫 번째 컬럼 값(조건에 해당하는 행(row)의 총 개수)만 가져옴
    print(f"[{table}] {start} ~ {end} 사이 행 수: {row_count}")

    # 전체 바이트를 MB로 환산
    estimated_mb = row_count * avg_len / 1024 / 1024

    # 소수점 2자리까지 반올림해서 반환
    return round(estimated_mb, 2)

'''
    날짜 객체(datetime)를 안전하게 문자열로 변환해주는 함수
    date_obj가 datetime일 수도 있고 그냥 문자열일 수도 있기 때문에, AttributeError 방지용 안전한 날짜 포맷 처리 함수
'''
def safe_date_format(date_obj):
    '''
        hasattr(date_obj, 'strftime'): 이 객체(date_obj)가 strftime() 메서드를 가지고 있는지 확인합니다. 즉, datetime 객체인지 확인하는 용도
        date_obj.strftime('%Y-%m-%d'): datetime 객체라면 "2024-12-31" 같은 YYYY-MM-DD 포맷 문자열로 변환
        else str(date_obj): datetime이 아니면 그냥 문자열로 바꿔(str())버립니다.
    '''
    return date_obj.strftime('%Y-%m-%d') if hasattr(date_obj, 'strftime') else str(date_obj)

# 메인 실행 함수
def main():
    # conn: 데이터베이스 연결 객체
    conn = get_connection()

    # 커서
    cursor = conn.cursor()

    # 데이터베이스 명
    db_name = conn.db.decode() if isinstance(conn.db, bytes) else conn.db

    # 테이블 명 가져오기
    tables = get_table_list(cursor, db_name)

    final_result = {}
    skipped = {}

    for table in tables:
        # 데이터 타입이 datetime 또는 timestamp인 컬럼명만 가져오기
        datetime_col = get_datetime_column(cursor, db_name, table)

        # 테이블에 datetime 또는 timestamp 컬럼이 하나도 없을 경우에 그 테이블을 분석에서 "건너뛰도록" 처리하는 조건
        if not datetime_col: # datetime_col이 None일 때 True가 되어 실행(건너뜀 처리)
            reason = "datetime 컬럼 없음"
            print(f"건너뜀: {table} ({reason})")
            skipped[table] = reason
            continue

        print(f"테이블 분석 중: {table} (기준 컬럼: {datetime_col})")

        # 최소값(가장 오래된 날짜)"과 "최대값(가장 최신 날짜)을 구해서 가져오기
        start_date, end_date = get_date_range(cursor, db_name, table, datetime_col)

        if not start_date or not end_date: # 날짜가 없거나 이상하면
            reason = "날짜 정보 없음 또는 이상값 존재"
            print(f"건너뜀: {table} ({reason})")
            skipped[table] = reason
            continue

        # 평균 한 행(row)이 차지하는 바이트 크기(Byte) 가져오기
        avg_len = get_avg_row_length(cursor, db_name, table)

        if avg_len == 0:
            reason = "AVG_ROW_LENGTH = 0"
            print(f"건너뜀: {table} ({reason})")
            skipped[table] = reason
            continue

        table_result = {
            "startDate": safe_date_format(start_date), # 시작 날짜를 문자열로 변환
            "endDate": safe_date_format(end_date), # 종료 날짜를 문자열로 변환
            "week": {},
            "month": {},
            "year": {}
        }

        for unit in ['week', 'month', 'year']: # 주 단위(week), 월 단위(month), 연 단위(year)로 반복
            for label, s, e in date_range_by_unit(start_date, end_date, unit): # 해당 단위로 날짜 범위를 나누기
                mb = estimate_storage(cursor, db_name, table, datetime_col, s, e, avg_len) # datetime_col 기준으로 s(시작일) ~ e(종료일) 범위의 행 개수를 센 다음, row 개수 × 평균 행 크기 → 바이트 → MB 로 변환
                if mb > 0: # 추정된 용량 mb가 0보다 클 경우만 저장
                    # f"{mb:,.3f}": 소수점 셋째자리까지, 천 단위 콤마 포함된 문자열로 변환
                    table_result[unit][label] = f"{mb:,.3f}"

        #  테이블별로 계산한 결과를 final_result 딕셔너리에 저장
        final_result[table] = table_result

    if skipped:
        final_result["skipped"] = skipped

    cursor.close()
    conn.close()

    '''
        json.dumps(...): 파이썬 객체 → JSON 문자열로 변환
        indent=2: 보기 좋게 줄 맞춤 (2칸 들여쓰기)
        ensure_ascii=False: 한글이 유니코드가 아닌 그대로 출력되도록
        print(...): 최종 결과 문자열을 터미널에 출력
    '''
    output = json.dumps(final_result, indent=2, ensure_ascii=False)

    print("\n결과값:")
    print(output)

    # 분석 결과를 현재 시각을 포함한 파일 이름으로 .json 파일로 저장
    filename = f"db_size_estimate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    '''
        open(filename, "w", encoding="utf-8")
            위에서 만든 이름으로 파일을 **쓰기 모드(w)**로 엶
            한글 등의 문자가 포함될 수 있으므로 encoding="utf-8" 사용
        with ... as f:
            파일을 열고 자동으로 닫아주는 안전한 파일 처리 방식
        f.write(output)
            JSON으로 만든 문자열 데이터를 파일에 기록함
    '''
    with open(filename, "w", encoding="utf-8") as f:
        f.write(output)

    print(f"\n분석 완료! 결과 파일: {filename}")
    print(f"파일 경로: {os.path.abspath(filename)}")

if __name__ == "__main__":
    main()
