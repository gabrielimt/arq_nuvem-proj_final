# Hiper Chat Local

## Arquitetura do Sistema

```mermaid
graph TD
    User["Navegador Web <br/>(Front-end / HTML5 + WebSockets)"] -->|"WebSockets (Socket.io)<br/>porta 3000"| ChatApp

    subgraph Docker_Network ["Rede Docker Compose"]
        ChatApp["chat-app (Node.js)<br/>- Servidor Express<br/>- Produtor/Consumidor Kafka<br/>- Coletor de Métricas (/metrics)"]
        Postgres[("PostgreSQL<br/>(chatdb)<br/>- Tabela: user_logins<br/>- Porta: 5432")]
        Zookeeper["Zookeeper<br/>(Coordenação)"]
        Kafka["Apache Kafka<br/>(Broker de Mensagens<br/>Tópico: chat-messages)<br/>- Porta: 9092 / 29092"]
        KafkaExporter["Kafka Exporter<br/>(Métricas Kafka)<br/>- Porta: 9308"]
        Prometheus["Prometheus<br/>(Coletor de Séries Temporais)<br/>- Porta: 9090"]
        Grafana["Grafana<br/>(Dashboard de Monitoramento)<br/>- Porta: 3001"]

        ChatApp -->|"Insere Login / Horário"| Postgres
        ChatApp -->|"Publica / Consome Mensagens"| Kafka
        Kafka -->|"Coordenação"| Zookeeper
        KafkaExporter -->|"Lê métricas do cluster"| Kafka
        Prometheus -->|"Scrape /metrics"| ChatApp
        Prometheus -->|"Scrape porta 9308"| KafkaExporter
        Grafana -->|"Consulta Prometheus"| Prometheus
    end

    Admin["Administrador"] -->|"Acessa Dashboard<br/>porta 3001"| Grafana
```

## Como Executar

1. Pare e remova os containers antigos:
   ```bash
   docker compose down
   ```
2. Suba a nova arquitetura completa:
   ```bash
   docker compose up --build -d
   ```

## URLs e Acessos

- **Chat Web:** `http://localhost:3000`
- **API de Logins Registrados (PostgreSQL):** `http://localhost:3000/api/logins`
- **Grafana Dashboard:** `http://localhost:3001` (Usuário: `admin` | Senha: `admin`)
- **Prometheus:** `http://localhost:9090`
- **PostgreSQL:** `localhost:5432` (Database: `chatdb` | Usuário: `chatuser` | Senha: `chatpassword`)

## Matriz RACI da Equipe

| Atividade | Cristiano | Gabrieli | Nathan | Patric |
|:---:|:---:|:---:|:---:|:---:|
| Desenvolvimento da aplicação | A/R | I | I | I |
| Configuração do Docker | C | I | A/R | C |
| Implementação do Terraform (IaC) | I | C | A/R | C |
| Deploy na nuvem | C | I | A/R | C |
| Implementação de mensageria ou serverless | R | I | C | A |
| Configuração de logs e métricas | C | I | C | A/R |
| Criação do dashboard | I | C | C | A/R |
| Testes de funcionamento e integração | R | A | C | C |
| Testes de disponibilidade e escalabilidade | C | I | A/R | R |
| README e documentação técnica | C | A/R | C | C |
| Registro de logs, eventos e evidências | I | A | C | R |
| Preparação dos slides | C | A/R | C | C |
| Apresentação e revisão final | R | A | R | R |