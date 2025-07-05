# 🧠 Smartsheet Job Scheduler Service

Este proyecto es un **servicio automático en Python** que se conecta con **Smartsheet API** para revisar y ejecutar jobs programados por empresa, usando configuraciones personalizadas por fila desde una hoja de control.

## ⚙️ ¿Qué hace este proyecto?

- Revisa una hoja de configuración en Smartsheet.
- Ejecuta tareas por empresa en intervalos definidos por cada una.
- Guarda en la hoja la **última ejecución** y calcula la **siguiente ejecución**.
- Usa `asyncio` para correr los jobs sin bloquear el flujo.
- Loguea toda la actividad para monitorear cuándo se ejecuta cada empresa.
- Funciona como microservicio con `FastAPI` + `Uvicorn`.

---

## 🧰 Tecnologías usadas

- 🐍 Python 3.11+
- 📋 FastAPI
- 📡 Smartsheet SDK
- 🕓 ZoneInfo (`tzdata` para zona horaria)
- 🔁 Asyncio
- 🗃️ Uvicorn

---

## 📦 Instalación

```bash
# Clona el repo
git clone https://github.com/tuusuario/nombre-del-repo.git
cd nombre-del-repo

# Crea entorno virtual
python -m venv .venv
source .venv/Scripts/activate  # En Windows

# Instala dependencias
pip install -r requirements.txt
