# Parsing Access log with databases

## Requirements

You need the followin to be able to run this code:



## Usage

First install the script and it's requirements:

```
git clone https://github.com/fricker12/DataBasesPytonParsingLog
cd DataBasesPythonParsingLog

```
Then run the script as follows:
```
Примеры запуска команд

python run.py --database MongoDB --mongodb_uri mongodb://localhost:27017 --db_name access_log --log_file access_log --analyze ip_user_agent

python run.py --database MySQL --host localhost --port 3306 --username root --password 12345678 --db_name access_log --log_file access_log --analyze ip_user_agent

python run.py --database Redis --host localhost --port 6379 --db_index 2 --log_file access_log

python run.py --database Redis --host localhost --port 6379 --db_index 2 --log_file access_log --analyze ip_user_agent

пример команды выполненя запроса к базе данных 

python databases.py mysql --host localhost --port 3306 --username root --password 12345678 --db-name access_log ip_user_agent_statistics --n 10

get_query_frequency

python <имя_файла>.py <тип_базы_данных> --host <хост> --port <порт> --username <имя_пользователя> --password <пароль> --db-name <имя_базы_данных> query_frequency --dT <число>

Эта функция get_query_frequency принимает соединение connection с базой данных и интервал времени dT в минутах. Она выполняет SQL-запрос, который выбирает дату и время (timestamp) из таблицы log_data, форматирует его в формате "ГГГГ-ММ-ДД ЧЧ:ММ" и подсчитывает количество записей для каждого интервала времени. Результат сортируется по возрастанию даты и времени.

top_user_agents

Эта функция get_top_user_agents принимает соединение connection с базой данных и число N, указывающее количество наиболее часто встречающихся User-Agent'ов, которые нужно вернуть. Она выполняет SQL-запрос, который выбирает столбец user_agent из таблицы log_data, подсчитывает количество вхождений для каждого User-Agent'а и сортирует результаты по убыванию частоты. Затем используется оператор LIMIT для ограничения количества записей, возвращаемых запросом, до N.

status_code_statistics

Эта функция get_status_code_statistics принимает соединение connection с базой данных и значение dT, указывающее интервал времени в минутах. Она выполняет SQL-запрос, который выбирает столбец status_code из таблицы log_data, подсчитывает количество вхождений для каждого статусного кода, начинающегося с '5' (50x ошибки), и ограничивает результаты по временному интервалу. Затем результаты группируются по статусному коду.

longest_requests или shortest_requests

Эта функция get_longest_shortest_requests принимает соединение connection с базой данных, значение N для указания количества запросов, а также флаг longest, который определяет, будут ли возвращены самые длинные запросы (если True) или самые короткие запросы (если False). Функция выполняет SQL-запрос, который выбирает столбец request из таблицы log_data и сортирует результаты по длине запроса в порядке убывания (для самых длинных запросов) или возрастания (для самых коротких запросов). Затем ограничивает результаты до указанного значения N.

common_requests

Эта функция get_common_requests принимает соединение connection с базой данных, значение N для указания количества запросов, а также значение slash_count, определяющее номер косой черты, до которой нужно анализировать запросы. В данном случае, для 2-й косой черты, значение slash_count будет равно 2. Функция выполняет SQL-запрос, который выбирает подстроку запроса до указанной косой черты с использованием функции SUBSTRING_INDEX, а затем считает частоту появления каждого уникального шаблона запроса с помощью функции COUNT(*). Результаты сортируются по убыванию частоты и ограничиваются указанным значением N.

upstream_requests_WORKER

Эта функция get_upstream_requests_WORKER принимает соединение connection с базой данных и выполняет SQL-запрос, который выбирает имя апстрима (BALANCER_WORKER_NAME) и считает количество запросов и среднее время выполнения (TIME) для каждого уникального апстрима. Результаты группируются по имени апстрима.

conversion_statistics

Эта функция get_conversion_statistics принимает соединение connection с базой данных и аргумент sort_by, определяющий поле для сортировки результатов (domain или conversion_count). SQL-запрос выбирает подстроку после третьего символа / в поле Referer, что является доменом, а затем считает количество записей для каждого уникального домена. Результаты группируются по домену и сортируются в соответствии с выбранным критерием.

upstream_requests

Эта функция get_upstream_requests принимает соединение connection с базой данных и аргумент interval, определяющий интервал времени для анализа количества исходящих запросов (в формате, например, '30 SECOND', '1 MINUTE', '5 MINUTE'). SQL-запрос выбирает метку времени в формате год-месяц-день час:минута:секунда и подсчитывает количество записей для каждого уникального момента времени в заданном интервале. Результаты группируются по метке времени и сортируются в хронологическом порядке.

most_active_periods

Эта функция find_most_active_periods принимает соединение connection с базой данных и аргумент N, определяющий количество временных интервалов для анализа. SQL-запрос формирует периоды, объединяя год-месяц-день час и округляя минуты до ближайшего множителя dT (в минутах). Затем подсчитывается количество записей для каждого периода, результаты сортируются по количеству запросов в убывающем порядке и ограничиваются N записями.