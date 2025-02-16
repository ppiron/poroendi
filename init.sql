create database airflow_db;
create user airflow_user with password 'airflow_pass';
grant all privileges on database airflow_db to airflow_user;
alter database airflow_db owner to airflow_user;

create database ny_taxi;
create user data_loader with password 'data_loader_pass';
grant all privileges on database ny_taxi to data_loader;
alter database ny_taxi owner to data_loader;
grant pg_read_server_files to data_loader;