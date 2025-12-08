# 📡 Terra Signal Copilot - Databricks App

Este diretório contém o código-fonte (`app.py`) e as dependências da aplicação front-end desenvolvida para o **Databricks Hackathon**.

## ⚠️ Contexto de Execução (Deployment)

O **Terra Signal Copilot** foi desenvolvido e implantado utilizando o recurso **Databricks Apps**. 

Embora os arquivos estejam disponíveis neste repositório para fins de avaliação, auditoria e revisão da lógica utilizada, a aplicação foi projetada para rodar nativamente dentro do ambiente Databricks, onde possui:

1.  **Integração Nativa:** Acesso direto ao **Unity Catalog** para leitura e escrita (Write-Back).
2.  **Segurança:** Uso de *Service Principals* e segredos gerenciados pelo Databricks.
3.  **Backend:** Processamento via Databricks SQL Serverless Warehouses e AI Functions.

### Stack Utilizada
* **Frontend:** Streamlit
* **Plataforma:** Databricks Apps
* **Dados & Governança:** Unity Catalog