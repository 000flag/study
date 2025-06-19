import pymysql
import json

def get_connection():
    # 데이터베이스 연결 정보 입력받기
    host = input("DB 호스트: ")
    user = input("DB 사용자명: ")
    password = input("DB 비밀번호: ")
    db = input("DB 이름: ")
    port = int(input("포트 (기본 3306): ") or "3306")

    # 데이터베이스 연결 정보 리턴
    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=db,
        port=port,
        charset='utf8'
    )

def get_datetime_columns():
    # conn: 데이터베이스 연결 객체
    conn = get_connection()

    # 커서
    cursor = conn.cursor()

    # 데이터베이스 명
    '''
        a if 조건 else b: 조건이 참이면 a 반환, 거짓이면 b 반환

        conn.db.decode() if isinstance(conn.db, bytes) else conn.db
        conn.db가 bytes면 → .decode()해서 str로 변환, 아니면 그대로 사용
    '''
    db_name = conn.db.decode() if isinstance(conn.db, bytes) else conn.db

    # 이 SQL은 MySQL에서 현재 데이터베이스에 있는 테이블 이름을 조회하는 쿼리
    ''' 쿼리 결과 예시
        +------------------+
        | Tables_in_mydb   |
        +------------------+
        | users            |
        | orders           |
        | products         |
        +------------------+
    '''
    cursor.execute(f"SHOW TABLES FROM `{db_name}`")

    '''
        fetchall()은 한 번만 호출!
        결과를 전부 가져오는 함수, 딱 한 번만 사용 가능
        다시 말해 cursor.fetchall()은 커서 내부에 임시 저장된 쿼리 결과를 한 번에 몽땅 꺼내서 비워버리는 함수
        그래서 cursor.fetchall()을 한 번 호출하면, 그 다음부터는 아무것도 안 남아 있음
        결과를 몽땅 한 번에 rows 리스트에 저장
    '''
    rows = cursor.fetchall()
    
    # 테이블 명 출력
    # fetchall()은 이 테이블을 파이썬의 리스트 형태로 변환
    ''' 변환 결과 예시
        [
            ('users',),
            ('orders',),
            ('products',)
        ]
    '''
    for row in rows:
        print(f">>> 테이블 이름: {row[0]}")

    # 테이블 명을 리스트에 저장
    tables = [row[0] for row in rows]

    result = {}

    # 테이블 컬럼 명, 컬럼 속성 조회
    for table in tables:
        # 실행된 결과는 cursor 내부 버퍼(메모리 공간)에 저장
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (db_name, table))

        # fetchall()을 호출하면 방금 실행된 SQL의 결과를 파이썬 리스트 형태로 추출
        rows = cursor.fetchall()

        print(f">>> {table} 테이블의 datetime 컬럼 조회 결과:")
        for row in rows:
            print(f"    컬럼명: {row[0]}, 타입: {row[1]}")  # 컬럼 이름과 타입 확인

        # 데이터 타입이 datetime 또는 timestamp인 컬럼명만 추출
        datetime_cols = [
            row[0] for row in rows
            if row[1].lower() in ("datetime", "timestamp")
        ]

        # 해당 테이블에 컬럼 명, 컬럼 속성 저장(JSON)
        result[table] = datetime_cols

    cursor.close()
    conn.close()

    '''
        json.dumps(...): 파이썬 객체 → JSON 문자열로 변환
        indent=2: 보기 좋게 줄 맞춤 (2칸 들여쓰기)
        ensure_ascii=False: 한글이 유니코드가 아닌 그대로 출력되도록
        print(...): 최종 결과 문자열을 터미널에 출력
    '''
    print(json.dumps(result, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    get_datetime_columns()