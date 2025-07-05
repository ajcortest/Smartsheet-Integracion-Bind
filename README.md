# Smartsheet → JSON API

Micro‑servicio en FastAPI que recibe el **ID** de una hoja de Smartsheet y devuelve:

```json
{
  "header": ["Columna1", "Columna2", "..."],
  "data": [
    { "Columna1": "valor", "Columna2": "valor", "...": "..." }
  ]
}
```

## Archivos
| Archivo | Descripción |
|---------|-------------|
| `main.py` | Código principal con FastAPI + logging |
| `requirements.txt` | Dependencias de Python |
| `Dockerfile` | Contenedor listo para producción |
| `README.md` | Esta guía rápida |

## Uso rápido

```bash
export SMARTSHEET_TOKEN="TU_TOKEN"
uvicorn main:app --reload --port 8080
curl http://localhost:8080/sheet/123456789
```

### Docker

```bash
docker build -t smartsheet-svc:0.1 .
docker run -d -p 8080:8080 -e SMARTSHEET_TOKEN=$SMARTSHEET_TOKEN smartsheet-svc:0.1
```

### Despliegue en Oracle Cloud

1. Sube la imagen a OCIR.  
2. Lanza una VM o Container Instance con la variable `SMARTSHEET_TOKEN`.  
3. Abre el puerto 8080 y consume:

```bash
curl http://IP_PUBLICA:8080/sheet/123456789
```
