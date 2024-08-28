import os
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql import DataFrame

def flatten_schema(df: DataFrame) -> DataFrame:
    """
    주어진 DataFrame에서 중첩된 구조체(StructType) 필드를 모두 최상위 필드로 평탄화합니다.
    """

    while True:
        has_struct = False  # StructType이 존재하는지 여부를 확인하는 플래그
        
        for field in df.schema.fields:
            # StructType 필드인지 확인
            if isinstance(field.dataType, StructType):
                has_struct = True  # StructType 필드가 있음을 표시
                # 모든 컬럼을 선택하고, 중첩된 필드를 최상위 레벨 컬럼으로 추가
                expanded_cols = [
                    col(f"{field.name}.{nested_field.name}").alias(f"{field.name}_{nested_field.name}")
                    for nested_field in field.dataType.fields
                ]
                # 기존 컬럼에 새로 평탄화된 컬럼들을 추가하고 원래의 StructType 컬럼은 삭제
                df = df.select("*", *expanded_cols).drop(field.name)
                break  # 스키마가 변경되었으므로 다시 처음부터 확인하기 위해 루프를 종료
            
            # ArrayType이면서, 그 내부가 StructType인 경우 먼저 배열을 풀어낸 후 처리
            elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                has_struct = True  # 처리할 StructType 필드가 있음을 표시
                # 배열을 풀어내기 위해 explode_outer 적용
                df = df.withColumn(field.name, explode_outer(field.name))
                break  # 스키마가 변경되었으므로 다시 처음부터 확인하기 위해 루프를 종료
                
        # 더 이상 StructType 필드가 없으면 반복을 종료
        if not has_struct:
            break
    
    return df



if __name__ == "__main__":
    # SparkSession 생성
    spark = SparkSession.builder.appName("Flatten Movie Data").getOrCreate()

    # JSON 파일 로드 경로 설정, DataFrame으로 로드
    
    input_path = os.path.expanduser('~/data/movbotdata/movieList.json')
    jdf = spark.read.option("multiline", "true").json(input_path)  
    
    # DataFrame 평탄화
    flattened_df = flatten_schema(jdf)

    # 데이터프레임을 로컬로 수집
    data = flattened_df.collect()

    # JSON 형식으로 변환하여 UTF-8로 저장
    output_path = os.path.expanduser('~/data/movbotdata/flatmovieList.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump([row.asDict() for row in data], f, ensure_ascii=False, indent=4)

    # 작업 종료
    spark.stop()

