# TCloud Core

Foundation 1 do backend central do TCloud.

## Agora

- Rust + Axum
- GET /health
- GET /api/v1/status
- GET /api/v1/files
- GET /api/v1/files/{id}
- modelos iniciais de arquivos
- CORS local
- sem credenciais no codigo

## Proximas fases

1. PostgreSQL e schema canonico
2. sessoes/dispositivos
3. integracao Telegram/grammers
4. fila de sincronizacao
5. delta/revisoes
6. WebSocket
7. compartilhamento
8. streaming autenticado

As credenciais reais nunca devem ser commitadas.