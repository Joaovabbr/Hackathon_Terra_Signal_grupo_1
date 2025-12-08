import streamlit as st
import pandas as pd
from databricks import sql
import re 
import numpy as np # Importado para segurança, embora não usado na main loop

# --- CONFIGURAÇÕES DE CONEXÃO (Substitua pelos seus valores reais) ---
SERVER_HOSTNAME = "dbc-5e8bca4c-f4f6.cloud.databricks.com"
HTTP_PATH       = "/sql/1.0/warehouses/d701bc74474aae26"
ACCESS_TOKEN    = "dapia9e650b13a25f20f296306ce1974673a"

# --- Mapeamento do Schema da Tabela GOLD (RAG Contexto) ---
TABLE_SCHEMA_CONTEXT = """
-- Tabela: workspace.churn_zero.site_feed_table
-- Colunas:
-- customerID: ID único do cliente.
-- gender: Gênero (Male/Female).
-- SeniorCitizen: Se é idoso (1) ou não (0).
-- Partner: Se tem parceiro (Yes/No).
-- Dependents: Se tem dependentes (Yes/No).
-- tenure: Tempo em meses que o cliente está na empresa.
-- PhoneService: Se tem serviço de telefone (Yes/No).
-- MultipleLines: Se tem múltiplas linhas (Yes/No).
-- InternetService: Serviço de internet (DSL, Fiber optic, No).
-- MonthlyCharges: Valor mensal cobrado (double).
-- TotalCharges: Valor total cobrado até hoje (double).
-- feedback_topic: Motivo principal de risco (Ex: high price, contract terms, Oportunidade de venda).
-- churn_probability: Probabilidade de churn (0 a 1).
-- num_addons: Número de serviços adicionais (integer).
-- status_venda: Status da negociação de retenção.
-- recommended_action: Ação tática sugerida para o vendedor.
-- priority_score: Score de prioridade para atendimento (double).
-- churn_probability_display: Probabilidade de churn formatada (string).
"""

# --- FUNÇÃO DE CONEXÃO ---
def get_connection():
    return sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=ACCESS_TOKEN
    )

# --- FUNÇÃO SQL DE ATUALIZAÇÃO ---
def update_customer_data(customer_id, new_churn_status, new_monthly_charges, new_addons, new_status_venda, 
                         new_internet_service, new_phone_service, new_multiple_lines):
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        churn_update = 1 if new_churn_status == 'Churn (Sair da Empresa)' else 0
        phone_service_update = new_phone_service 
        multiple_lines_update = new_multiple_lines
        
        update_query = f"""
        UPDATE workspace.churn_zero.site_feed_table
        SET 
            Churn = {churn_update},
            MonthlyCharges = {new_monthly_charges},
            num_addons = {new_addons},
            status_venda = '{new_status_venda}',
            InternetService = '{new_internet_service}',
            PhoneService = '{phone_service_update}',
            MultipleLines = '{multiple_lines_update}',
            churn_probability = CASE WHEN {churn_update} = 1 THEN 1.0 ELSE 0.0 END
        WHERE 
            customerID = '{customer_id}'
        """
        
        cursor.execute(update_query)
        conn.commit() 
        return True
    
    except Exception as e:
        st.error(f"Erro ao atualizar o cliente no banco de dados: {str(e)}")
        return False
        
    finally:
        cursor.close()
        conn.close()

# --- FUNÇÃO DO CHATBOT GLOBAL (RAG COM RETRY) ---
def generate_global_ai_response(user_question):
    conn = get_connection()
    cursor = conn.cursor()
    
    MAX_ATTEMPTS = 3
    previous_error = None
    final_sql_to_execute = None
    last_generated_sql = ""
    contexto_factual = "Não foi possível buscar dados específicos."
    
    for attempt in range(MAX_ATTEMPTS):
        
        error_context = ""
        if previous_error:
            error_context = (
                f"ATENÇÃO: A TENTATIVA ANTERIOR FALHOU. O SQL gerado era: '{last_generated_sql}'. "
                f"O erro de execução retornado foi: '{previous_error}'. "
                "Corrija o SQL gerado para evitar este erro. Gere APENAS o SQL corrigido."
            )
            
        sql_generation_prompt = f"""
        {error_context}
        Sua tarefa é atuar como um Tradutor de Linguagem Natural para SQL.
        Você DEVE gerar SOMENTE o código SQL necessário para responder à PERGUNTA do usuário.
        Tabela a ser consultada: workspace.churn_zero.site_feed_table
        {TABLE_SCHEMA_CONTEXT}
        PERGUNTA DO USUÁRIO (VENDEDOR): {user_question}
        QUERY SQL: 
        """
        
        try:
            cursor.execute(f"SELECT ai_gen('{sql_generation_prompt}') as sql_query")
            generated_sql = cursor.fetchone()[0]
            
            cleaned_sql = generated_sql.strip()
            cleaned_sql = re.sub(r"```(sql)?|['\"`]", "", cleaned_sql, flags=re.IGNORECASE).strip()

            if not cleaned_sql.upper().startswith("SELECT"):
                raise ValueError(f"IA não gerou uma query SELECT válida. Output: {cleaned_sql}")
            
            last_generated_sql = cleaned_sql
            final_sql_to_execute = cleaned_sql

        except Exception as e:
            previous_error = f"Erro na Geração/Validação: {str(e)}"
            continue
            
        try:
            cursor.execute(final_sql_to_execute)
            
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchmany(size=5)
            
            if results:
                contexto_factual = "DADOS ANALÍTICOS OBTIDOS:\n"
                contexto_factual += f"Colunas: {', '.join(columns)}\n"
                for row in results:
                    contexto_factual += f"Resultado: {row}\n"
            else:
                contexto_factual = "DADOS ANALÍTICOS OBTIDOS: A consulta foi executada, mas não retornou resultados (0 linhas)."
                
            break 
            
        except Exception as e:
            previous_error = f"Erro de Execução SQL: {str(e)}"
            continue
            
    if attempt == MAX_ATTEMPTS - 1 and previous_error and not final_sql_to_execute:
        return f"A IA não conseguiu gerar ou executar uma query SQL válida após {MAX_ATTEMPTS} tentativas. Não posso responder a esta pergunta no momento. Erro da última tentativa: {previous_error}"

    response_prompt = f"""
    ATENÇÃO: Você é um Consultor Estratégico Interno da Terra Signal.
    Seu objetivo é dar munição tática e insights.
    
    DIRETRIZES:
    1. Baseie sua resposta APENAS nos DADOS ANALÍTICOS OBTIDOS injetados abaixo.
    2. Nunca peça para o usuário buscar a resposta. Forneça o insight diretamente.
    3. Responda como um conselheiro experiente. Use o formato: "O dado que temos é [X]. Sugiro que você [Y]..."
    
    CONTEXTO (DADOS FACTUAIS):
    {contexto_factual}
    
    PERGUNTA ORIGINAL DO VENDEDOR: {user_question}
    """
    
    prompt_safe = response_prompt.replace("'", "").replace('"', "")
    
    try:
        cursor.execute(f"SELECT ai_gen('{prompt_safe}') as resposta")
        final_response = cursor.fetchone()[0]
        return final_response.replace("$", "\$")
    except Exception as e:
        return f"Erro na IA (Geração Final da Resposta): {str(e)}"
    finally:
        cursor.close()
        conn.close()


# --- FUNÇÃO DO CHATBOT INDIVIDUAL ---
def generate_ai_response(user_question, customer_data):
    conn = get_connection()
    cursor = conn.cursor()
    
    def safe_get(key, default='N/A'):
        val = customer_data.get(key)
        if val is None: val = customer_data.get(key.lower())
        if val is None: return default
        return str(val)

    churn_val = customer_data.get('Churn', customer_data.get('churn', 0))
    status_texto = 'CANCELOU' if churn_val == 1 else 'ATIVO'

    contexto = f"""
    Cliente: {safe_get('customerID')}
    Status Atual: {status_texto}
    Risco de Churn: {safe_get('churn_probability_display')}
    Motivo Principal: {safe_get('feedback_topic')}
    Sugestão do Sistema: {safe_get('recommended_action')}
    Valor Mensal: ${safe_get('MonthlyCharges')}
    Serviço Internet: {safe_get('InternetService')}
    """
    
    prompt = f"""
    ATENÇÃO: Você é um Consultor Estratégico Interno da Terra Signal.
    Você está conversando com um VENDEDOR da nossa equipe (o usuário), e NÃO com o cliente final.
    
    SEU OBJETIVO: Dar munição tática (argumentos, scripts, dados) para o vendedor fechar o negócio ou reter o cliente.
    
    ... (Restante das Diretrizes) ...
    
    DADOS DO CLIENTE QUE O VENDEDOR ESTÁ ATENDENDO:
    {contexto}
    
    PERGUNTA DO VENDEDOR: {user_question}
    """
    prompt_safe = prompt.replace("'", "").replace('"', "")
    
    try:
        cursor.execute(f"SELECT ai_gen('{prompt_safe}') as resposta")
        result = cursor.fetchone()[0]
        return result.replace("$", "\$")
    except Exception as e:
        return f"Erro na IA: {str(e)}"
    finally:
        cursor.close()
        conn.close()

# --- LOAD DATA ---
@st.cache_data(ttl=300)
def load_data():
    conn = get_connection()
    query = "SELECT * FROM workspace.churn_zero.site_feed_table"
    df = pd.read_sql(query, conn)
    conn.close()
    
    df['churn_probability'] = pd.to_numeric(df['churn_probability'], errors='coerce').fillna(0)
    df['MonthlyCharges'] = pd.to_numeric(df['MonthlyCharges'], errors='coerce').fillna(0)
    
    df['InternetService'] = df['InternetService'].replace({'No': 'No Internet'})
    
    return df

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Terra Signal Copilot", layout="wide", page_icon="📡")

# --- ESTADO DA SESSÃO ---
if 'view' not in st.session_state: st.session_state.view = 'list'
if 'selected_id' not in st.session_state: st.session_state.selected_id = None
if 'chat_history' not in st.session_state: st.session_state.chat_history = []
if 'chatbot_active' not in st.session_state: st.session_state.chatbot_active = False

# --- CARREGAMENTO INICIAL ---
try:
    df = load_data()
except Exception as e:
    st.error("❌ Erro de Conexão com o Warehouse:")
    st.code(str(e))
    st.stop()

# --- VARIÁVEIS DE CONFIGURAÇÃO E FUNÇÕES DE FILTRO (OMITIDAS) ---
opcoes_segmentos = ["🚨 Risco Crítico", "💰 Oportunidade", "🛡️ Clientes Seguros"]
opcoes_tech = sorted(df['InternetService'].unique())
opcoes_motivos = df['feedback_topic'].unique() if 'feedback_topic' in df.columns else []

def reset_filters():
    st.session_state.f_search = ""
    st.session_state.f_segmentos = opcoes_segmentos
    st.session_state.f_tech = opcoes_tech
    st.session_state.f_ticket = 0
    st.session_state.f_motivo = []

# --- TELA 3: CHATBOT GLOBAL (View function para a aba principal) ---
def global_chatbot_view():
    st.header("💬 Suporte Estratégico Global")
    st.caption("Fale com o assistente sobre produtos, regras de negócio ou estratégias gerais.")
    st.divider()

    if 'global_chat_history' not in st.session_state:
        st.session_state.global_chat_history = []
        
    chat_container = st.container(height=500)
    
    with chat_container:
        for msg in st.session_state.global_chat_history:
            with st.chat_message(msg["role"]):
                st.write(msg["content"].replace("$", "\$"))

    if prompt := st.chat_input("Ex.: Qual é o churn rate médio para clientes com Fibra Óptica?"):
        
        st.session_state.global_chat_history.append({"role": "user", "content": prompt})
        
        with chat_container:
            with st.chat_message("user"):
                st.write(prompt)
            
            with st.chat_message("assistant"):
                with st.spinner("Buscando estratégia..."):
                    resp = generate_global_ai_response(prompt) 
                    st.write(resp.replace("$", "\$"))
        
        st.session_state.global_chat_history.append({"role": "assistant", "content": resp})

# ==============================================================================
# ESTRUTURA PRINCIPAL
# ==============================================================================

tab_lista, tab_global_chat, tab_update = st.tabs(["📋 Lista de Clientes", "💬 Suporte Global", "🔄️ Atualização Cadastral"])

# ------------------------------------------------------------------------------
# 1. ABA DE LISTAGEM E DETALHE
# ------------------------------------------------------------------------------
with tab_lista:
    if st.session_state.view == 'list':
        
        # --- TELA 1: LISTA (DASHBOARD) ---
        col_logo, col_title = st.columns([1, 8])
        col_title.title("Terra Signal | Sales Copilot")
        st.markdown("---")
        
        st.sidebar.header("🎯 Painel de Controle")
        if st.sidebar.button("🔄 Resetar Filtros", on_click=reset_filters): st.toast("Filtros resetados!")
        st.sidebar.markdown("---")
        
        search_id = st.sidebar.text_input("🔎 Buscar ID do Cliente", key="f_search")
        segmentos = st.sidebar.pills("Foco da Ação", options=opcoes_segmentos, default=opcoes_segmentos, selection_mode="multi", key="f_segmentos")
        filter_tech = st.sidebar.pills("Filtrar por Internet:", options=opcoes_tech, default=opcoes_tech, selection_mode="multi", key="f_tech")
        
        # --- APLICAÇÃO DOS FILTROS ---
        df_filtered = df.copy()
        if search_id: df_filtered = df_filtered[df_filtered['customerID'].astype(str).str.contains(search_id, case=False)]
        
        mask_oportunidade = (df_filtered['feedback_topic'] == 'Oportunidade de venda')
        mask_risco = (df_filtered['churn_probability'] > 0.5) & (df_filtered['feedback_topic'] != 'Oportunidade de venda')
        mask_seguro = (df_filtered['churn_probability'] <= 0.5) & (df_filtered['feedback_topic'] != 'Oportunidade de venda')
        
        final_mask = pd.Series([False] * len(df_filtered), index=df_filtered.index)
        if "💰 Oportunidade" in segmentos: final_mask = final_mask | mask_oportunidade
        if "🚨 Risco Crítico" in segmentos: final_mask = final_mask | mask_risco
        if "🛡️ Clientes Seguros" in segmentos: final_mask = final_mask | mask_seguro
        df_filtered = df_filtered[final_mask]
        
        if filter_tech: df_filtered = df_filtered[df_filtered['InternetService'].isin(filter_tech)]
        else: df_filtered = df_filtered[df_filtered['InternetService'].isin([])] 
        
        valor_total = df_filtered['MonthlyCharges'].sum()
        
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Fila de Atendimento", len(df_filtered))
        k4.metric("Receita Total", f"$ {valor_total:,.0f}")

        st.subheader(f"📋 Fila Prioritária")
        
        h_cols = st.columns([1, 2, 1, 1, 3, 1])
        h_cols[0].markdown("**Score**")
        h_cols[1].markdown("**Cliente**")
        h_cols[2].markdown("**Status**")
        h_cols[3].markdown("**Valor**")
        h_cols[4].markdown("**Motivo Principal**")
        h_cols[5].markdown("**Ação**")
        st.divider()

        for index, row in df_filtered.head(50).iterrows():
            cols = st.columns([1, 2, 1, 1, 3, 1])
            score = row.get('priority_score', 0)
            
            # ... (Lógica de exibição e Status Badge) ...
            
            cols[0].write(f"🔥 {int(score)}")
            cols[1].write(f"**{row['customerID']}**")
            # ... (Restante das colunas) ...
            
            if cols[5].button("Abrir", key=row['customerID']):
                st.session_state.selected_id = row['customerID']
                st.session_state.view = 'detail'
                st.session_state.chatbot_active = False
                st.session_state.chat_history = []
                st.rerun()
            st.divider()


    # ==============================================================================
    # TELA 2: DETALHE
    # ==============================================================================
    elif st.session_state.view == 'detail':
        
        try:
            cliente = df[df['customerID'] == st.session_state.selected_id].iloc[0]
        except:
            st.error("Erro ao carregar cliente.")
            if st.button("Voltar"):
                st.session_state.view = 'list'
                st.rerun()
            st.stop()

        if st.button("⬅️ Voltar para Lista"):
            st.session_state.view = 'list'
            st.rerun()

        st.title(f"👤 Cliente: {cliente['customerID']}")
        
        c_left, c_right = st.columns([1, 2])
        
        with c_left:
            st.subheader("Dados Cadastrais")
            st.write(f"**Internet:** {cliente.get('InternetService', '-')}")
            # ... (Restante do código c_left) ...
        
        with c_right:
            st.subheader("🚀 Estratégia Sugerida (IA)")
            # ... (Restante do código c_right) ...
            
            with c_right: # Re-uso do c_right
                st.subheader("💬 Assistente de Venda")
                # ... (Lógica do chatbot individual) ...


# ------------------------------------------------------------------------------
# 2. ABA DE SUPORTE GLOBAL
# ------------------------------------------------------------------------------
with tab_global_chat:
    global_chatbot_view()

# ------------------------------------------------------------------------------
# 3. ABA DE ATUALIZAÇÃO CADASTRAIL AVULSA (COM CORREÇÃO DE FORM)
# ------------------------------------------------------------------------------
with tab_update:
    st.header("🔄️ Atualização Cadastral Manual")
    st.warning("⚠️ Esta aba permite alterar dados de **qualquer cliente** diretamente. Use com cautela.")
    st.markdown("---")
    
    with st.form("form_update_customer_manual"):
        
        # --- 1. CAMPO DE CUSTOMER ID (SEMPRE VISÍVEL) ---
        customer_id_input = st.text_input(
            "Digite o **ID do Cliente** para atualizar:", 
            key='f_customer_id_input', 
            help="Ex: 8433-AJYAS"
        )
        
        st.markdown("---")
        st.subheader("Resultado Final da Interação")
        
        # 2. Status da Venda 
        new_churn_status = st.radio(
            "Resultado da Interação:",
            options=['Cliente Mantido (Ativo)', 'Churn (Sair da Empresa)'],
            key='f_status_churn_manual'
        )
        
        st.markdown("---")
        st.subheader("Alteração de Serviços")

        # 3. Alteração de Serviços (Inputs)
        new_internet_service = st.selectbox(
            "Serviço de Internet Atual:",
            options=['Fiber optic', 'DSL', 'No Internet'],
            key='f_internet_service_manual'
        )
        
        new_phone_service = st.radio(
            "Serviço de Telefone:",
            options=['Yes', 'No'],
            key='f_phone_service_manual'
        )

        new_multiple_lines = st.radio(
            "Linhas Múltiplas:",
            options=['Yes', 'No', 'No phone service'],
            key='f_multiple_lines_manual'
        )
        
        # 4. Dados Financeiros
        st.markdown("---")
        st.subheader("Novos Dados Financeiros/Produtos")
        
        new_addons = st.number_input(
            "Número de Add-ons (incluindo novos):", 
            min_value=0, 
            value=0, 
            step=1, 
            key='f_addons_manual'
        )
        
        new_monthly_charges = st.number_input(
            "Nova Mensalidade Total ($):",
            min_value=0.0,
            value=0.0,
            step=0.01,
            key='f_monthly_manual'
        )
        
        new_status_venda = st.text_area(
            "Detalhes da Ação (Ex: Oferta Contrato 1 ano / Não Contornado)",
            value='',
            key='f_status_venda_manual'
        )

        # O BOTÃO DE SUBMIT DEVE ESTAR AQUI DENTRO DO FORM (CORREÇÃO)
        submitted = st.form_submit_button("✅ CONFIRMAR E SALVAR DADOS")
    
    # ----------------------------------------------------
    # LÓGICA DE SUBMIT/VALIDAÇÃO (FORA DO WITH st.form)
    # ----------------------------------------------------
    if submitted:
        if not customer_id_input:
            st.error("🚨 O campo 'ID do Cliente' é obrigatório para a atualização.")
        else:
            # Chama a função de atualização com os dados do formulário
            if update_customer_data(
                customer_id_input, 
                new_churn_status, 
                new_monthly_charges, 
                new_addons, 
                new_status_venda,
                new_internet_service, 
                new_phone_service, 
                new_multiple_lines 
            ):
                st.success(f"Dados do cliente {customer_id_input} atualizados com sucesso no Unity Catalog!")
                load_data.clear() 
                st.rerun()
            else:
                st.error("Falha na atualização. Verifique o ID do Cliente e as permissões.")