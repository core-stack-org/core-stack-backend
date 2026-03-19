param(
    [string]$CondaEnvName = "corestackenv",
    [string]$PostgresUser = "corestack_admin",
    [string]$PostgresDb = "corestack_db",
    [string]$PostgresPassword = "corestack@123",
    [string]$PostgresHost = "127.0.0.1",
    [string]$PostgresPort = "5432",
    [string]$PostgresAdminUser = "",
    [string]$PostgresAdminPassword = "",
    [string]$PostgresAdminDatabase = "postgres",
    [switch]$SkipCondaEnvCreate,
    [switch]$SkipPostgresSetup,
    [switch]$SkipSeedData,
    [switch]$SkipValidation
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BackendDir = (Resolve-Path (Join-Path $ScriptDir "..")).Path
$EnvYaml = Join-Path $ScriptDir "environment.yml"
$BootstrapScript = Join-Path $ScriptDir "bootstrap_env.py"
$SeedData = Join-Path $ScriptDir "seed\seed_data.json"

function Find-Conda {
    $condaCommand = Get-Command conda -ErrorAction SilentlyContinue
    if ($condaCommand) {
        return $condaCommand.Source
    }

    $candidates = @(
        (Join-Path $env:USERPROFILE "miniconda3\Scripts\conda.exe"),
        (Join-Path $env:USERPROFILE "anaconda3\Scripts\conda.exe"),
        (Join-Path $env:LOCALAPPDATA "miniconda3\Scripts\conda.exe"),
        (Join-Path $env:LOCALAPPDATA "anaconda3\Scripts\conda.exe")
    )

    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path $candidate)) {
            return $candidate
        }
    }

    throw "Conda was not found. Install Miniconda/Anaconda first, then reopen PowerShell."
}

function Invoke-Conda {
    param([string[]]$Args)

    & $script:CondaExe @Args
    if ($LASTEXITCODE -ne 0) {
        throw "Conda command failed: conda $($Args -join ' ')"
    }
}

function Get-EnvExists {
    $envListJson = & $script:CondaExe env list --json
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to list Conda environments."
    }

    $envList = $envListJson | ConvertFrom-Json
    return @($envList.envs | Where-Object { (Split-Path $_ -Leaf) -eq $CondaEnvName }).Count -gt 0
}

function Setup-PostgresRoleAndDb {
    $psql = Get-Command psql -ErrorAction SilentlyContinue
    if (-not $psql) {
        Write-Warning "psql was not found on PATH. Skipping PostgreSQL role/database setup."
        return
    }

    if (-not $PostgresAdminUser -or -not $PostgresAdminPassword) {
        Write-Warning "Skipping PostgreSQL role/database setup. Supply -PostgresAdminUser and -PostgresAdminPassword to automate it."
        return
    }

    $env:PGPASSWORD = $PostgresAdminPassword

    try {
        $roleExists = & $psql.Source -h $PostgresHost -p $PostgresPort -U $PostgresAdminUser -d $PostgresAdminDatabase -tAc "SELECT 1 FROM pg_roles WHERE rolname = '$PostgresUser';"
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to connect to PostgreSQL with the supplied admin credentials."
        }

        if (-not ([string]$roleExists).Trim()) {
            & $psql.Source -h $PostgresHost -p $PostgresPort -U $PostgresAdminUser -d $PostgresAdminDatabase -v ON_ERROR_STOP=1 -c "CREATE ROLE `"$PostgresUser`" WITH LOGIN PASSWORD '$PostgresPassword';"
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to create PostgreSQL role $PostgresUser."
            }
        } else {
            & $psql.Source -h $PostgresHost -p $PostgresPort -U $PostgresAdminUser -d $PostgresAdminDatabase -v ON_ERROR_STOP=1 -c "ALTER ROLE `"$PostgresUser`" WITH LOGIN PASSWORD '$PostgresPassword';"
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to update PostgreSQL role $PostgresUser."
            }
        }

        $dbExists = & $psql.Source -h $PostgresHost -p $PostgresPort -U $PostgresAdminUser -d $PostgresAdminDatabase -tAc "SELECT 1 FROM pg_database WHERE datname = '$PostgresDb';"
        if (-not ([string]$dbExists).Trim()) {
            & $psql.Source -h $PostgresHost -p $PostgresPort -U $PostgresAdminUser -d $PostgresAdminDatabase -v ON_ERROR_STOP=1 -c "CREATE DATABASE `"$PostgresDb`" OWNER `"$PostgresUser`";"
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to create PostgreSQL database $PostgresDb."
            }
        }

        & $psql.Source -h $PostgresHost -p $PostgresPort -U $PostgresAdminUser -d $PostgresDb -v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS postgis;"
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "PostGIS extension could not be created automatically. Ensure PostGIS is installed for database $PostgresDb."
        }
    }
    finally {
        Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
    }
}

if ($env:OS -notlike "*Windows*") {
    throw "install_windows.ps1 must be run from native Windows PowerShell."
}

$script:CondaExe = Find-Conda

Write-Host "Using backend directory: $BackendDir"
Write-Host "Using Conda executable: $script:CondaExe"

if (-not $SkipCondaEnvCreate) {
    if (Get-EnvExists) {
        Invoke-Conda @("env", "update", "-f", $EnvYaml, "-n", $CondaEnvName, "--prune")
    } else {
        Invoke-Conda @("env", "create", "-f", $EnvYaml, "-n", $CondaEnvName)
    }
}

Invoke-Conda @(
    "run",
    "-n",
    $CondaEnvName,
    "python",
    $BootstrapScript,
    "--backend-dir",
    $BackendDir,
    "--db-name",
    $PostgresDb,
    "--db-user",
    $PostgresUser,
    "--db-password",
    $PostgresPassword,
    "--db-host",
    $PostgresHost,
    "--db-port",
    $PostgresPort,
    "--celery-worker-pool",
    "solo"
)

if (-not $SkipPostgresSetup) {
    Setup-PostgresRoleAndDb
}

if (-not (Get-Command rabbitmqctl -ErrorAction SilentlyContinue)) {
    Write-Warning "rabbitmqctl was not found on PATH. Install/start RabbitMQ before running the Celery worker."
}

Push-Location $BackendDir
try {
    Invoke-Conda @("run", "-n", $CondaEnvName, "python", "manage.py", "collectstatic", "--noinput", "--skip-checks")
    Invoke-Conda @("run", "-n", $CondaEnvName, "python", "manage.py", "migrate", "--skip-checks")

    if ((-not $SkipSeedData) -and (Test-Path $SeedData)) {
        Invoke-Conda @("run", "-n", $CondaEnvName, "python", "manage.py", "loaddata", "--skip-checks", $SeedData)
    }

    if (-not $SkipValidation) {
        Invoke-Conda @("run", "-n", $CondaEnvName, "python", "computing/misc/internal_api_initialisation_test.py")
    }
}
finally {
    Pop-Location
}

Write-Host ""
Write-Host "Windows setup complete."
Write-Host "Run the API server with:"
Write-Host "  conda run -n $CondaEnvName python manage.py runserver"
Write-Host ""
Write-Host "Run the Celery worker with:"
Write-Host "  conda run -n $CondaEnvName celery -A nrm_app worker -l info -Q nrm --pool=solo"
Write-Host ""
Write-Host "Create a Django superuser when needed with:"
Write-Host "  conda run -n $CondaEnvName python manage.py createsuperuser --skip-checks"
