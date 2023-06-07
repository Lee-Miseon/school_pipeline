from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1, tzinfo=local_tz),
    'retries': 0
}

day = DAG(
    'gen_table',
    default_args=default_args,
    schedule_interval="0 8 * * 1-5"
)

time = DAG(
    'attendance_check',
    default_args=default_args,
    schedule_interval="0 8-16 * * 1-5"
)

def gen_day_task(name, cmd, dags, trigger='all_success'):
    task = BashOperator(
        task_id=name,
        bash_command=cmd,
        trigger_rule=trigger,
        dag=dags
    )
    return task

data_1 = gen_day_task('get_student', 'echo "get"', day)
data_2 = gen_day_task('get_timetable', 'echo "get"', day)
data_3 = gen_day_task('data_load', 'echo "hdfs"', day)
data_4 = gen_day_task('table_join', 'echo "join"', day)
data_5 = gen_day_task('make_col', 'echo "make"', day)

[data_1, data_2] >> data_3 >> data_4 >> data_5

check_1 = gen_day_task('get_attendance', 'echo "get"', time)
check_2 = gen_day_task('noti_to_teacher', 'echo "noti"', time)
check_3 = gen_day_task('attendance_check', 'echo "check"', time)

check_1 >> check_3
check_1 >> check_2
