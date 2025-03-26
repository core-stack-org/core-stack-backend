# CoRE Stack Backend Setup

Branch: develop 

# MySQL DB Setup
1. sudo apt-get update
2. sudo apt-get install mysql-server

*Start MySQL*
1. sudo service mysql start

*Create a new database*
1. mysql -u root -p or sudo mysql (if no password is setup)
2. create database nrm;

*Create a new user*
1. mysql -u root -p
2. CREATE USER 'nrm_user'@'localhost' IDENTIFIED BY 'xxxxxx';
3. GRANT ALL PRIVILEGES ON nrm.* TO 'nrm_user'@'localhost';
4. FLUSH PRIVILEGES;

# Installation of the libraries
1. chmod +x install.sh
2. ./install.sh

*Provide a name of your virtual environment when prompted: 
> example: nrm

*Provide a path to your environment when prompted: 
> example: /home/user/virtualenvs/nrm

Let the script run and install all the dependencies.

# Running the server
After the successfull installation of all the packages, run the following commands to start the Django server:
1. conda activate nrm (or whatever is the name of your virtual environment)
2. python manage.py runserver 

*Make Migrations*
1. Make sure the environment is active. Otherwise, run `conda activate nrm`
2. Run `python manage.py migrate`
For any migration related queries, please send an email to contact@core-stack.org

*Run the server*
1. Make sure the environment is active. Otherwise, run `conda activate nrm`
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