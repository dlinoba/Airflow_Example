# Airflow_Example
--To create MYSQL Container use the Dockerfile and docker-compose

--To create AIRFLOW Container
docker run -d -p 8080:8080 -v "$PWD/airflow/dags:/opt/airflow/dags/" --entrypoint=/bin/bash --name airflow apache/airflow:2.1.1-python3.8 -c '(airflow db init && airflow users create --username admin --password 1234 --firstname Simple --lastname Example --role Admin --email admin@example.com.br); airflow webserver & airflow scheduler'

After everything is running, you can check mysql database and check DIM_CLIENTE table
Next insert a fake sales in FATO table by sql command bellow

INSERT INTO FATO VALUES (1,1,1,1,1,1,350,10,10,10,11)

And then insert a new client or change a client by sql command bellow
INSERT INTO CLIENTE (IDCLIENTE,NOME,SOBRENOME,EMAIL,SEXO,NASCIMENTO)
VALUES (11,'FAKE','NAME','FakeName@supermail.com','M','1989-05-22');

UPDATE CLIENTE SET NOME = 'TEST' WHERE IDCLIENTE = 1
