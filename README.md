# CoRE Stack Backend Setup

# DB Setup
Install PostgreSQL
1. sudo apt-get update
2. sudo apt-get install postgresql postgresql-contrib

*Start PostgreSQL*
1. sudo service postgresql start

*Check PostgreSQL status*
1. sudo systemctl status postgresql

*Create a new database*
1. sudo -u postgres psql
2. CREATE DATABASE corestack;

*Create a new user*
1. sudo -u postgres psql
2. create user corestack_user with password 'your_password';
3. GRANT USAGE, CREATE ON SCHEMA public TO corestack_user;
4. ALTER DATABASE corestack OWNER TO corestack_user;
5. ALTER USER corestack_user WITH SUPERUSER; (Optional, only if you want superuser rights)

# Installation of the libraries
If you want to install directly from a conda snap of environment.yml
> conda env create -f environment.yml

If you want to install using the install.sh script
1. chmod +x install.sh
2. ./install.sh

*Provide a name of your virtual environment when prompted: 
> example: corestack

*Provide a path to your environment when prompted: 
> default: /home/user/miniconda/envs/corestack
> example: /home/user/virtualenvs/corestack

Let the script run and install all the dependencies.

# Running the server
After the successfull installation of all the packages, run the following commands to start the Django server:
1. conda activate corestack (or whatever is the name of your virtual environment)
2. python manage.py runserver 

*Make Migrations*
1. Make sure the environment is active. Otherwise, run `conda activate corestack`
2. Run `python manage.py migrate`
For any migration related queries, please send an email to contact@core-stack.org
2. Run `python manage.py migrate`
For any migration related queries, please send an email to contact@core-stack.org

*Run the server*
1. Make sure the environment is active. Otherwise, run `conda activate corestack`
2. Run `python manage.py runserver`
> Tip: To make local hits to your server, use `python manage.py runserver 0.0.0.0:8080`

*Running celery*
If you are running some tasks, you need to run `celery -A tasks worker -l info`
> example: `celery -A nrm_app worker -l info -Q nrm &` 
nrm queue is running in this example.

# Env and config setup
1. Use .env.example as a template to create .env file inside nrm_app/ directory
2. Update the environment variables
3. Add *JSON* files inside the data/ directory

# More
- [DB Design](https://github.com/core-stack-org/core-stack-backend/wiki/DB-Design) 
- [API Doc](https://github.com/core-stack-org/core-stack-backend/wiki/Project-API-Doc)