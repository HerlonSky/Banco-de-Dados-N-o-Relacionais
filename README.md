# HerlON Flow

> Um pedido, dois caminhos, um único evento.

Projeto individual desenvolvido por **Herlon**.

O HerlON Flow é uma API FastAPI para cadastro e consulta de pedidos. Os dados
são persistidos no MongoDB e cada cadastro gera a mesma ocorrência lógica em
dois canais: uma notificação curta no RabbitMQ e um evento completo no Kafka.

## Diferencial da solução

Em vez de produzir duas mensagens sem relação entre si, a API cria um envelope
de evento próprio. O mesmo `evento_id` acompanha a mensagem nos dois brokers,
permitindo rastrear o caminho do pedido. O envelope também possui versão, origem
e data da ocorrência.

```text
                         +--> RabbitMQ: notificação enxuta
POST /pedidos --> MongoDB|
                         +--> Kafka: evento completo
                              ^
                              | mesmo evento_id
```

Todas as respostas também recebem o cabeçalho `X-OrderFlow-Request-ID`, útil
para localizar uma requisição nos logs.

## Executar todo o ambiente

Pré-requisito: Docker com Docker Compose.

```bash
docker compose up --build
```

Após a inicialização:

- Documentação Swagger: http://localhost:8000/docs
- API: http://localhost:8000
- Gerenciador RabbitMQ: http://localhost:15672 (`appuser` / `apppassword`)
- MongoDB: `localhost:27017`
- Kafka: `localhost:9092`

## Endpoints

### Criar um pedido

`POST /pedidos`

```json
{
  "nome_cliente": "Giovanna",
  "nome_produto": "Headset Bluetooth",
  "quantidade": 2
}
```

Neste exemplo, `Giovanna` é apenas uma cliente fictícia. Outros exemplos e
testes também podem usar `Wictoria`; a autoria do projeto continua sendo
exclusivamente de Herlon.

O identificador, o status `PENDENTE` e a data de criação são gerados pela API.

### Listar pedidos

`GET /pedidos`

### Verificar a API

`GET /health`

## Eventos publicados

- RabbitMQ: fila durável `pedidos.criados`, com o identificador e o status.
- Kafka: tópico `pedidos-criados`, com os dados completos do pedido; o ID é a
  chave da mensagem, preservando a ordenação de eventos do mesmo pedido.

Exemplo do cabeçalho compartilhado:

```json
{
  "evento_id": "b1a8c26e-91ec-4a3e-9c64-66f0eb9b43cc",
  "tipo": "pedido.criado",
  "versao": "1.0",
  "origem": "br.com.herlon.orderflow",
  "ocorrido_em": "2026-06-22T18:30:00Z"
}
```

## Executar os testes

Os testes usam repositório e publicadores em memória, sem exigir infraestrutura.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

Para desligar os contêineres:

```bash
docker compose down
```

Use `docker compose down -v` somente quando também quiser apagar os dados
persistidos do MongoDB e RabbitMQ.
