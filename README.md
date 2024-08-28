### Installation
다음 명령어를 통해 레포지토리를 클론합니다.
```
$ git clone git@github.com:DoubleChat-Samdul/dags.git
```
클론한 레포지토리의 디렉토리로 이동한 후, 현재 디렉토리의 절대 경로를 확인합니다.
```
$ cd <CLONED_REPOSITORY>
$ pwd
```
출력된 절대 경로를 <path>로 설정한 후, AIRFLOW HOME 디렉토리의 airflow.cfg 파일에서 dags_folder 항목을 <PATH>로 변경합니다.
```
$ cat airflow.cfg | grep dags_folder
dags_folder = <PATH>
```
그 다음, airflow standalone 명령어를 실행하여 에어플로우 서버를 다시 시작하면, DAG는 <PATH>/dags 디렉토리에서 로드됩니다.

## Dags 기능
- 'time_dag.py'

 이 기능은 Airflow를 사용하여 매일 아침 9시 30분에 Kafka를 통해 채팅방에 미팅 5분 전 알림 메시지를 자동으로 전송합니다. Airflow에서 schedule_interval='30 00 * * *'로 설정된 크론 표현식은 기본적으로 UTC(협정 세계시) 기준으로 매일 0시 30분에 DAG가 실행됩니다. 한국 표준시(KST, UTC+9)로 변환하면, 이는 매일 오전 9시 30분에 해당하는 시간입니다.  알림 메세지는 PythonOperator를 사용하여 KafkaProducer 함수를 호출하며, 이 함수는 Kafka의 'team2' 라는 토픽으로 메시지를 전송하는 방식으로 구현되었습니다.

![image](https://github.com/user-attachments/assets/a3bbdf76-c42e-4430-acff-76f20d5fd339)  



- 'movbot.py'

이 기능은 영화진흥위원회 OPEN API를 활용하여 영화 목록 데이터를 JSON 파일로 저장합니다. 이후, PySpark의 explode_outer 함수를 사용하여 중첩된 필드나 배열 필드를 최상위 수준의 필드로 펼치고, 리스트나 중첩된 요소를 개별 행으로 분리된 데이터로 변환합니다. 이렇게 변환된 데이터는 영화 챗봇 기능에서 사용자가 요청한 질문에 대해 @bot이 정보를 검색할 때 활용됩니다. 해당 DAG 코드에서 py 디렉토리의 `flatten.py` 와 `movlist.py` 를 호출하기 때문에 해당 파일의 위치를 사용자의 경로에 맞게 변경해주어야 합니다.  
  
데이터 변환이 완료되면 Kafka를 통해 채팅방에 해당년도 영화목록 데이터 추출 작업이 완료되었다는 알림 메시지를 자동으로 전송합니다. 알림 메세지는 PythonOperator를 사용하여 KafkaProducer 함수를 호출하며, 이 함수는 Kafka의 'team2' 라는 토픽으로 메시지를 전송하는 방식으로 구현되었습니다.  

![image](https://github.com/user-attachments/assets/54b2d5fa-b6f5-415a-9365-75789b22e25b)
