import os
import json
import base64
import pymysql
import requests
from datetime import datetime, timezone
from flask import Flask, request, jsonify, Response

app = Flask(__name__)

# =========================
# ENV (Railway Variables)
# =========================
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "develop_1_lic")

WEBHOOK_AUTH = os.getenv("WEBHOOK_AUTH", "")  # ex: "Basic dev854850"
TECNOSPEED_BASE = os.getenv("TECNOSPEED_BASE", "https://pix.tecnospeed.com.br")

PLUGZ_API_URL = os.getenv("PLUGZ_API_URL", "")
PLUGZ_CLIENT_TOKEN = os.getenv("PLUGZ_CLIENT_TOKEN", "")

# =========================
# Tabelas fixas
# =========================
TBL_COBRANCAS = f"{DB_NAME}.pix_cobrancas_geradas"
TBL_RECEBIDOS = f"{DB_NAME}.pix_recebidos"
TBL_DADOSPIX = f"{DB_NAME}.dadospix"
TBL_EVENTOS = f"{DB_NAME}.pix_webhook_eventos"
TBL_AUTENTICACAO = f"{DB_NAME}.autenticacao"


# =========================
# DB Helpers
# =========================
def db_conn():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        port=DB_PORT,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_json(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"


def parse_iso_dt(s: str):
    """
    Converte '2021-08-25T00:19:23.248Z' ou '2019-08-24T14:15:22Z'
    -> 'YYYY-MM-DD HH:MM:SS' (UTC, sem tzinfo)
    """
    if not s:
        return None
    try:
        s2 = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        return dt.astimezone(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


# =========================
# Vinculo / schema / dados
# =========================
def buscar_vinculo_por_pix(cursor, pix_id: str) -> dict:
    """
    PIX gerado no seu sistema
    - id_cobrancas
    - codigoparasistema (codigoempresa do sistema/empresa)
    - codcadastro (cadastro do "cliente do sistema" dentro do schema)
    - pedidovendaid (para achar cliente final)
    """
    cursor.execute(
        f"""
        SELECT
          id_cobrancas,
          codigoparasistema,
          codcadastro,
          pedidovendaid
        FROM {TBL_COBRANCAS}
        WHERE pix_id = %s
        LIMIT 1
        """,
        (pix_id,),
    )
    row = cursor.fetchone()
    return row or {
        "id_cobrancas": None,
        "codigoparasistema": None,
        "codcadastro": None,
        "pedidovendaid": None,
    }


def buscar_schema_por_codigoempresa(cursor, codigoempresa: int) -> str | None:
    cursor.execute(
        f"""
        SELECT ESQUEMA
        FROM {TBL_AUTENTICACAO}
        WHERE CODIGOEMPRESA = %s
        LIMIT 1
        """,
        (codigoempresa,),
    )
    r = cursor.fetchone()
    if not r:
        return None
    schema = (r.get("ESQUEMA") or "").strip().lower()
    return schema or None


def buscar_dadospix(cursor, codigoparasistema, codcadastro) -> dict:
    """
    Busca credenciais/token no dadospix (pega o mais recente).
    """
    cursor.execute(
        f"""
        SELECT
          iddadospix,
          token_company,
          token_company_expires_at,
          tecnospeed_client_id,
          tecnospeed_client_secret
        FROM {TBL_DADOSPIX}
        WHERE
          (%s IS NULL OR codigoparasistema = %s)
          AND
          (%s IS NULL OR codcadastro = %s)
        ORDER BY iddadospix DESC
        LIMIT 1
        """,
        (codigoparasistema, codigoparasistema, codcadastro, codcadastro),
    )
    return cursor.fetchone() or {}


def token_expirado(expires_at) -> bool:
    """
    Considera expirado se faltam <120s
    """
    if not expires_at:
        return True
    try:
        dt = expires_at
        if isinstance(expires_at, str):
            dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (dt - now).total_seconds() < 120
    except Exception:
        return True


def renovar_token_company(client_id: str, client_secret: str) -> dict:
    """
    POST https://pix.tecnospeed.com.br/oauth2/token
    Headers:
      Authorization: Basic base64(client_id:client_secret)
      Content-Type: application/x-www-form-urlencoded
    Body:
      grant_type=client_credentials
      role=company
    """
    url = f"{TECNOSPEED_BASE}/oauth2/token"

    raw = f"{client_id}:{client_secret}".encode("utf-8")
    b64 = base64.b64encode(raw).decode("utf-8")

    headers = {
        "Authorization": f"Basic {b64}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    data = {"grant_type": "client_credentials", "role": "company"}

    r = requests.post(url, headers=headers, data=data, timeout=20)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Falha ao renovar token ({r.status_code}): {r.text}")

    j = r.json() or {}
    access_token = j.get("access_token")
    expires_in = int(j.get("expires_in") or 3600)

    if not access_token:
        raise RuntimeError(f"Resposta sem access_token: {r.text}")

    expires_ts = datetime.now(timezone.utc).timestamp() + expires_in
    expires_dt = datetime.fromtimestamp(expires_ts, tz=timezone.utc).replace(tzinfo=None)

    return {
        "access_token": access_token,
        "expires_at": expires_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "expires_in": expires_in,
    }


def garantir_token_company(cursor, codigoparasistema, codcadastro) -> str:
    dp = buscar_dadospix(cursor, codigoparasistema, codcadastro)
    if not dp:
        raise RuntimeError("Não encontrei registro em dadospix para esse vínculo (codigoparasistema/codcadastro).")

    token = (dp.get("token_company") or "").strip()
    expires_at = dp.get("token_company_expires_at")
    client_id = (dp.get("tecnospeed_client_id") or "").strip()
    client_secret = (dp.get("tecnospeed_client_secret") or "").strip()
    iddadospix = dp.get("iddadospix")

    if not token or token_expirado(expires_at):
        if not client_id or not client_secret:
            raise RuntimeError("client_id/client_secret ausentes no dadospix para renovar token_company.")
        novo = renovar_token_company(client_id, client_secret)
        token = novo["access_token"]

        cursor.execute(
            f"""
            UPDATE {TBL_DADOSPIX}
            SET token_company=%s,
                token_company_expires_at=%s
            WHERE iddadospix=%s
            """,
            (token, novo["expires_at"], iddadospix),
        )

    return token


# =========================
# TecnoSpeed: Consultar PIX por ID
# =========================
def tecnospeed_consultar_pix_por_id(pix_id: str, token_company: str) -> dict:
    """
    GET https://pix.tecnospeed.com.br/api/v1/pix/{id}
    """
    url = f"{TECNOSPEED_BASE}/api/v1/pix/{pix_id}"
    headers = {"Authorization": f"Bearer {token_company}", "Accept": "application/json"}

    r = requests.get(url, headers=headers, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Consulta PIX por ID falhou ({r.status_code}): {r.text}")

    return r.json() or {}


# =========================
# Inserts / Updates
# =========================
def inserir_evento(cursor, event_name: str, pix_id: str, headers_json: dict, json_completo: dict):
    cursor.execute(
        f"""
        INSERT INTO {TBL_EVENTOS}
          (event_name, pix_id, headers_json, json_completo, recebido_em)
        VALUES
          (%s, %s, %s, %s, %s)
        """,
        (str(event_name or ""), str(pix_id or ""), safe_json(headers_json), safe_json(json_completo), now_str()),
    )


def extrair_payer(pix_full: dict) -> tuple[str, str]:
    """
    Alguns retornos vêm com payerCpfCnpj/payerName, outros com effectivePayer.*
    """
    cpfcnpj = (pix_full.get("payerCpfCnpj") or "").strip()
    nome = (pix_full.get("payerName") or "").strip()

    eff = pix_full.get("effectivePayer") or {}
    if not cpfcnpj:
        cpfcnpj = (eff.get("cpfCnpj") or eff.get("payerCpfCnpj") or "").strip()
    if not nome:
        nome = (eff.get("name") or eff.get("payerName") or "").strip()

    return cpfcnpj, nome


def is_pago_confirmado(pix_full: dict) -> bool:
    """
    Se tiver paymentDate OU status indicar liquidado/pago, consideramos pago confirmado.
    """
    status = str(pix_full.get("status") or "").upper().strip()
    payment_date = pix_full.get("paymentDate")
    if payment_date:
        return True
    return status in ("LIQUIDATED", "PAID")


def upsert_pix_recebido(cursor, pix_full: dict, vinculo: dict):
    pix_id = str(pix_full.get("id") or "")
    surrogate = str(pix_full.get("surrogateKey") or "")
    status = str(pix_full.get("status") or "")
    amount = pix_full.get("amount")
    payment_date = parse_iso_dt(pix_full.get("paymentDate"))
    payer_doc, payer_name = extrair_payer(pix_full)
    emv = str(pix_full.get("emv") or "")
    created_at_api = parse_iso_dt(pix_full.get("createdAt"))

    codigoparasistema = vinculo.get("codigoparasistema")
    codcadastro = vinculo.get("codcadastro")
    id_cobrancas = vinculo.get("id_cobrancas")

    pago = 1 if is_pago_confirmado(pix_full) else 0

    cursor.execute(f"SELECT id_recebido FROM {TBL_RECEBIDOS} WHERE pix_id=%s LIMIT 1", (pix_id,))
    exists = cursor.fetchone()

    if exists:
        cursor.execute(
            f"""
            UPDATE {TBL_RECEBIDOS}
            SET
              surrogate_key=%s,
              status=%s,
              amount=%s,
              payment_date=%s,
              payer_cpf_cnpj=%s,
              payer_name=%s,
              emv=%s,
              created_at_api=%s,
              recebido_em=%s,
              json_completo=%s,
              codigoparasistema=COALESCE(%s, codigoparasistema),
              codcadastro=COALESCE(%s, codcadastro),
              id_cobrancas=COALESCE(%s, id_cobrancas),
              pago=GREATEST(pago, %s)
            WHERE pix_id=%s
            """,
            (
                surrogate,
                status,
                amount,
                payment_date,
                payer_doc,
                payer_name,
                emv,
                created_at_api,
                now_str(),
                safe_json(pix_full),
                codigoparasistema,
                codcadastro,
                id_cobrancas,
                pago,
                pix_id,
            ),
        )
        return int(exists["id_recebido"])
    else:
        cursor.execute(
            f"""
            INSERT INTO {TBL_RECEBIDOS}
              (pix_id, surrogate_key, status, amount, payment_date, payer_cpf_cnpj, payer_name, emv, created_at_api,
               recebido_em, codigoparasistema, codcadastro, id_cobrancas, pago, json_completo)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s,
               %s,%s,%s,%s,%s,%s)
            """,
            (
                pix_id,
                surrogate,
                status,
                amount,
                payment_date,
                payer_doc,
                payer_name,
                emv,
                created_at_api,
                now_str(),
                codigoparasistema,
                codcadastro,
                id_cobrancas,
                pago,
                safe_json(pix_full),
            ),
        )
        return cursor.lastrowid


# =========================
# WhatsApp PlugzAPI
# =========================
def montar_lista_telefones(cadastro_row: dict) -> list[str]:
    """
    Monta lista de destinos usando ddd1/fone1 ... ddd5/fone5
    Retorna como "55<ddd><fone>" (somente dígitos)
    """
    phones = []
    for i in range(1, 6):
        ddd = (cadastro_row.get(f"ddd{i}") or "").strip()
        fone = (cadastro_row.get(f"fone{i}") or "").strip()
        if not ddd or not fone:
            continue
        digits = "".join(ch for ch in (ddd + fone) if ch.isdigit())
        if len(digits) >= 10:
            phones.append("55" + digits)
    # remove duplicados
    seen = set()
    out = []
    for p in phones:
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out


def enviar_whatsapp(telefone_destino: str, mensagem: str) -> bool:
    if not PLUGZ_API_URL or not PLUGZ_CLIENT_TOKEN:
        print("[WARN] PLUGZ_API_URL ou PLUGZ_CLIENT_TOKEN não configurados no Railway Variables.")
        return False

    payload = {"phone": telefone_destino, "message": mensagem}
    headers = {"Content-Type": "application/json", "Client-Token": PLUGZ_CLIENT_TOKEN}

    try:
        resp = requests.post(PLUGZ_API_URL, headers=headers, json=payload, timeout=20)
        print(f"✅ WhatsApp enviado para {telefone_destino}. Status: {resp.status_code}")
        print("📟 Resposta PlugzAPI:", resp.text)
        return resp.status_code in (200, 201)
    except Exception as e:
        print(f"❌ Erro ao enviar WhatsApp para {telefone_destino}: {repr(e)}")
        return False


def gerar_mensagem_pagamento(nome_cliente_sistema: str, pedido_id: str, nome_cliente_final: str, valor, data_pagamento: str) -> str:
    valor_str = f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if valor is not None else "0,00"
    data_str = data_pagamento or "N/A"
    return (
        f"Olá, {nome_cliente_sistema}! ✅\n"
        f"O pagamento do pedido nº {pedido_id}, referente ao cliente {nome_cliente_final}, foi confirmado com sucesso.\n\n"
        f"💰 Valor: R$ {valor_str}\n"
        f"📅 Data: {data_str}"
    )


def enviar_notificacao_pagamento(cursor, pix_id: str, vinculo: dict, pix_full: dict):
    """
    Envia WhatsApp SOMENTE se pago confirmado.
    """
    if not is_pago_confirmado(pix_full):
        print(f"[INFO] PIX ainda não pago de verdade (pix_id={pix_id}). Não envia WhatsApp.")
        return

    codigoparasistema = vinculo.get("codigoparasistema")
    codcadastro_cliente_sistema = vinculo.get("codcadastro")
    pedidovendaid = vinculo.get("pedidovendaid")

    if not codigoparasistema:
        print(f"[WARN] Sem codigoparasistema no vínculo (pix_id={pix_id}).")
        return

    schema = buscar_schema_por_codigoempresa(cursor, int(codigoparasistema))
    if not schema:
        print(f"[WARN] Não achei schema em autenticacao para codigoempresa={codigoparasistema}.")
        return

    # 1) Cliente do sistema (quem recebe o aviso)
    cursor.execute(
        f"""
        SELECT razaosocial, ddd1, fone1, ddd2, fone2, ddd3, fone3, ddd4, fone4, ddd5, fone5
        FROM `{schema}`.cadastro
        WHERE codcadastro = %s
        LIMIT 1
        """,
        (codcadastro_cliente_sistema,),
    )
    cliente_sistema = cursor.fetchone() or {}
    nome_cliente_sistema = (cliente_sistema.get("razaosocial") or "Cliente").strip()
    telefones = montar_lista_telefones(cliente_sistema)

    if not telefones:
        print(f"[WARN] Nenhum telefone encontrado no cadastro do cliente do sistema (schema={schema}, codcadastro={codcadastro_cliente_sistema}).")
        return

    # 2) Cliente final via pedidovendaid
    nome_cliente_final = "Cliente Final"
    codcadastro_final = None
    if pedidovendaid:
        cursor.execute(
            f"SELECT codcadastro FROM `{schema}`.pedidovenda WHERE pedidovendaid=%s LIMIT 1",
            (pedidovendaid,),
        )
        r = cursor.fetchone()
        if r and r.get("codcadastro"):
            codcadastro_final = r["codcadastro"]

    if codcadastro_final:
        cursor.execute(
            f"SELECT razaosocial FROM `{schema}`.cadastro WHERE codcadastro=%s LIMIT 1",
            (codcadastro_final,),
        )
        r2 = cursor.fetchone()
        if r2 and r2.get("razaosocial"):
            nome_cliente_final = r2["razaosocial"]

    # 3) Valor/data do pagamento
    valor = pix_full.get("amount")
    data_pagamento = parse_iso_dt(pix_full.get("paymentDate"))

    msg = gerar_mensagem_pagamento(
        nome_cliente_sistema=nome_cliente_sistema,
        pedido_id=str(pedidovendaid or "N/A"),
        nome_cliente_final=nome_cliente_final,
        valor=valor,
        data_pagamento=data_pagamento,
    )

    print("[INFO] Mensagem WhatsApp gerada:")
    print(msg)

    # 4) Envia para todos os telefones
    for tel in telefones:
        enviar_whatsapp(tel, msg)


# =========================
# Routes
# =========================
@app.get("/")
def home():
    return jsonify({"service": "pix-webhook", "status": "ok"}), 200


@app.post("/webhook/pix-pago")
def webhook_pix():
    """
    Recebe evento TecnoSpeed:
      {"event": "PIX_SUCCESSFUL", "id": "<pix_id>"}
    """
    try:
        # valida Authorization
        auth = request.headers.get("Authorization", "")
        if WEBHOOK_AUTH and auth != WEBHOOK_AUTH:
            return jsonify({"error": "Unauthorized"}), 401

        payload = request.get_json(silent=True) or {}
        event_name = (payload.get("event") or payload.get("type") or "").strip()
        pix_id = (payload.get("id") or payload.get("pix_id") or "").strip()

        print("📨 Webhook recebido:")
        print(json.dumps(payload, ensure_ascii=False))

        if not pix_id:
            return jsonify({"error": "pix_id ausente no payload"}), 400

        headers_dict = {k: v for k, v in request.headers.items()}

        conn = db_conn()
        try:
            with conn.cursor() as cursor:
                # 1) salva auditoria SEMPRE
                inserir_evento(cursor, event_name, pix_id, headers_dict, payload)

                ev = event_name.upper().strip()

                # 2) só faz a rotina completa para eventos de "pago"
                if ev in ("PIX_SUCCESSFUL", "PIX_PAID"):
                    vinculo = buscar_vinculo_por_pix(cursor, pix_id)
                    if not vinculo.get("id_cobrancas"):
                        print(f"[WARN] Não encontrei pix_id na pix_cobrancas_geradas: {pix_id}")

                    # token_company do dadospix (renova se precisar)
                    token_company = garantir_token_company(
                        cursor,
                        vinculo.get("codigoparasistema"),
                        vinculo.get("codcadastro"),
                    )

                    # consulta completa por ID
                    pix_full = tecnospeed_consultar_pix_por_id(pix_id, token_company)

                    print("[INFO] Retorno TecnoSpeed /api/v1/pix/{id}:")
                    print(json.dumps(pix_full, ensure_ascii=False))

                    # grava/atualiza recebidos
                    upsert_pix_recebido(cursor, pix_full, vinculo)

                    # envia WhatsApp se pago de verdade
                    enviar_notificacao_pagamento(cursor, pix_id, vinculo, pix_full)

                conn.commit()

        finally:
            try:
                conn.close()
            except Exception:
                pass

        return jsonify({"ok": True}), 200

    except Exception as e:
        print("❌ ERRO WEBHOOK:", repr(e))
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/ui")
def ui():
    """
    UI simples:
      /ui -> últimos eventos/recebidos
      /ui?pix_id=... -> filtra por pix_id
    """
    pix_id = request.args.get("pix_id", "").strip()

    conn = db_conn()
    try:
        with conn.cursor() as cursor:
            if pix_id:
                cursor.execute(f"SELECT * FROM {TBL_EVENTOS} WHERE pix_id=%s ORDER BY id_evento DESC LIMIT 50", (pix_id,))
                eventos = cursor.fetchall()
                cursor.execute(f"SELECT * FROM {TBL_RECEBIDOS} WHERE pix_id=%s LIMIT 1", (pix_id,))
                recebidos = cursor.fetchone()
            else:
                cursor.execute(f"SELECT * FROM {TBL_EVENTOS} ORDER BY id_evento DESC LIMIT 50")
                eventos = cursor.fetchall()
                cursor.execute(f"SELECT * FROM {TBL_RECEBIDOS} ORDER BY id_recebido DESC LIMIT 50")
                recebidos = cursor.fetchall()

        html = f"""
        <html><head><meta charset="utf-8"><title>PIX Monitor</title></head>
        <body style="font-family:Arial;margin:20px;">
          <h2>PIX Monitor (Railway)</h2>
          <form method="get" action="/ui" style="margin-bottom:14px;">
            <label>Buscar por PIX ID:</label>
            <input name="pix_id" value="{pix_id}" style="width:520px;padding:6px;" />
            <button type="submit" style="padding:6px 10px;">Buscar</button>
            <a href="/ui" style="margin-left:10px;">Limpar</a>
          </form>
          <h3>Últimos Eventos (pix_webhook_eventos)</h3>
          <pre style="background:#f6f6f6;padding:10px;border-radius:8px;overflow:auto;max-height:360px;">{json.dumps(eventos, ensure_ascii=False, indent=2)}</pre>
          <h3>PIX Recebidos (pix_recebidos)</h3>
          <pre style="background:#f6f6f6;padding:10px;border-radius:8px;overflow:auto;max-height:360px;">{json.dumps(recebidos, ensure_ascii=False, indent=2)}</pre>
        </body></html>
        """
        return Response(html, mimetype="text/html")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)