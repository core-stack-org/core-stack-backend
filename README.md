# CoRE Stack Backend Setup

## 🚀 Automated Installation

We provide a single installation script that handles everything:  
- Installs **Miniconda** and sets up the Python environment  
- Installs & configures **PostgreSQL**  
- Installs & configures **Apache with mod_wsgi**  
- Clones the backend repo and applies Django **migrations**  
- Collects **static files**  
- Sets up **logs** and **Apache config**  

## 📝 Requirements

Before starting, make sure you have the following installed on your system:

- A **Linux-based operating system** (Ubuntu 20.04+ recommended)  
- **Git** (to clone the repository)  
- **Bash** (usually preinstalled on Linux)  

The installation script will handle the rest (Conda, PostgreSQL, Apache, etc.).


### 1. Clone the repository
```bash
git clone https://github.com/core-stack-org/core-stack-backend.git
cd core-stack-backend/installation
```

### 2. Run the installation script
```bash
chmod +x install.sh
./install.sh
```

> The script will automatically install Conda, PostgreSQL, Apache, set up the `corestack-backend` environment, run migrations, and configure Apache.


### 3. Open in Browser
- API Docs: [http://localhost](http://localhost)  
- Django Admin: [http://localhost/admin/](http://localhost/admin/)
---

## ⚙️ Configuration

1. Copy `.env.example` → `.env` inside `nrm_app/`  
2. Update database and service variables  
3. Place required **JSON files** inside the `data/` directory  

---

## 🖥️ Development Usage

If you want to run the server manually (instead of Apache):

```bash
# Activate environment
conda activate corestack-backend

# Apply migrations
python manage.py migrate

# Run Django development server
python manage.py runserver 0.0.0.0:8080
```

### Running Celery
If your project uses background tasks, you need to run Celery workers in parallel:

```bash
# Start Celery worker
celery -A nrm_app worker -l info -Q nrm
```

- `-A nrm_app` → points to your Django app (`nrm_app`)  
- `-Q nrm` → runs the worker on the **nrm** queue (update if you use other queues)  
- Add `&` at the end if you want it to run in the background  


## 🔗 Resources
- [DB Design](https://github.com/core-stack-org/core-stack-backend/wiki/DB-Design)  
- [API Documentation](https://github.com/core-stack-org/core-stack-backend/wiki/Project-API-Doc)  
