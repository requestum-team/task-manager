# Task Manager

## Quick start
```bash
# build
docker-compose build --no-cache
```
```bash
# up
docker-compose up
```
It starts `synthetic_test.py` by default.


## Docker env example
- POSTGRES_USER=task_manager
- POSTGRES_PASS=task_manager_password
- POSTGRES_NAME=task_manager
- POSTGRES_HOST=postgres
- POSTGRES_PORT=5432
- POSTGRES_HOST_AUTH_METHOD=trust