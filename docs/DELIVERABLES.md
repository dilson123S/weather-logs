# Entregables y Guía rápida

Este documento describe los entregables solicitados y los pasos para verificarlos.

## Entregables esperados

- Repositorio Git con código y `README.md` detallado. (✅ ya incluido)
- Video demostrativo publicado en el foro. (instrucciones abajo)
- Esquema visual del diseño en la documentación. (`docs/architecture.md`)
- `docker-compose.yml` y configuración de volúmenes. (✅ en repo)
- Scripts de inicialización de la base de datos. (`scripts/init_db.sh`, `scripts/init_db.ps1`)
- Documentación de uso y pruebas de validación. (`docs/DELIVERABLES.md`, `scripts/run_validation_tests.sh`)

## Cómo generar el video demostrativo

1. Levanta el sistema localmente:

```bash
cp .env.example .env
docker compose up -d --build
```

2. Graba un video de 2–5 minutos mostrando:
   - Acceso al RabbitMQ UI (http://localhost:15672)
   - Panel de Grafana (http://localhost:3000)
   - Ejecución de consultas en la BD (`make query-logs`)
   - Publicación/consumo de mensajes (logs de producer/consumer)

3. Exporta el video en MP4 y súbelo al foro asignado.

### Publicar en el foro

- En el post incluye: enlace al repositorio, duración del video, pasos para reproducir localmente, y resultados de las pruebas.

## Verificación rápida de entregables (comandos)

```bash
# Levantar servicios
docker compose up -d --build

# Ver logs de productor y consumidor
docker compose logs -f --tail=100 producer consumer

# Ejecutar script de inicialización (desde la máquina host)
./scripts/init_db.sh

# Ejecutar pruebas de validación
./scripts/run_validation_tests.sh
```

## Contacto
Para soporte usa el issue tracker del repositorio.
