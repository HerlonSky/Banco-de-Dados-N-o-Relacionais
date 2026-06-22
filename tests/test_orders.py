from fastapi.testclient import TestClient

from app.main import create_app
from tests.fakes import InMemoryOrderRepository, RecordingPublisher


def make_client():
    repository = InMemoryOrderRepository()
    rabbit = RecordingPublisher()
    kafka = RecordingPublisher()
    app = create_app(
        repository=repository,
        rabbit_publisher=rabbit,
        kafka_publisher=kafka,
    )
    return TestClient(app), repository, rabbit, kafka


def test_cadastra_pedido_com_status_pendente_e_publica_eventos():
    client, repository, rabbit, kafka = make_client()

    with client:
        response = client.post(
            "/pedidos",
            json={
                "nome_cliente": "Ana Souza",
                "nome_produto": "Teclado mecânico",
                "quantidade": 2,
            },
        )

    assert response.status_code == 201
    body = response.json()
    assert body["id"]
    assert body["nome_cliente"] == "Ana Souza"
    assert body["nome_produto"] == "Teclado mecânico"
    assert body["quantidade"] == 2
    assert body["status"] == "PENDENTE"
    assert len(repository.orders) == 1
    rabbit_event = rabbit.published[0]
    kafka_event = kafka.published[0]
    assert rabbit_event.pedido.id == body["id"]
    assert kafka_event.pedido.id == body["id"]
    assert rabbit_event.cabecalho.evento_id == kafka_event.cabecalho.evento_id
    assert rabbit_event.cabecalho.origem == "br.com.herlon.orderflow"


def test_lista_todos_os_pedidos_cadastrados():
    client, _, _, _ = make_client()

    with client:
        client.post(
            "/pedidos",
            json={
                "nome_cliente": "Giovanna",
                "nome_produto": "Mouse",
                "quantidade": 1,
            },
        )
        client.post(
            "/pedidos",
            json={
                "nome_cliente": "Wictoria",
                "nome_produto": "Monitor",
                "quantidade": 3,
            },
        )
        response = client.get("/pedidos")

    assert response.status_code == 200
    orders = response.json()
    assert len(orders) == 2
    assert [order["nome_cliente"] for order in orders] == ["Giovanna", "Wictoria"]
    assert all(order["status"] == "PENDENTE" for order in orders)


def test_rejeita_quantidade_invalida():
    client, _, _, _ = make_client()

    with client:
        response = client.post(
            "/pedidos",
            json={
                "nome_cliente": "Carlos",
                "nome_produto": "Cabo USB",
                "quantidade": 0,
            },
        )

    assert response.status_code == 422


def test_expoe_identidade_propria_do_projeto():
    client, _, _, _ = make_client()

    with client:
        response = client.get("/")

    assert response.status_code == 200
    assert response.json() == {
        "nome": "HerlON Flow",
        "slogan": "Um pedido, dois caminhos, um único evento.",
        "autor": "Herlon",
    }
    assert response.headers["X-OrderFlow-Request-ID"]
