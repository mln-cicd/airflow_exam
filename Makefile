
init-airflow:
	docker compose up airflow-init\
	&& docker-compose down --remove-orphans


init:
	mkdir -p logs && sudo chmod 777 logs\
	&& 	mkdir -p logs && sudo chmod 777 logs\
	&& 	mkdir -p plugins && sudo chmod 777 plugins\
	&& 	mkdir -p mount && sudo chmod 777 mount\
	&& $(MAKE) init-airflow


up:
	docker compose up

down:
	docker-compose down --remove-orphans

teardown:
	docker-compose down --remove-orphans -v