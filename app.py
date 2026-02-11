import os
import json
import base64
import re
from datetime import datetime, timezone, timedelta

import pymysql
import requests
from flask import Flask, request, jsonify, Response

# timezone BR
try:
    from zoneinfo import ZoneInfo
    TZ_BR = ZoneInfo("America/Sao_Paulo")
except Exception:
    TZ_BR = timezone(timedelta(hours=-3))

app = Flask(__name__)

# =========================
# ENV (Railway Variables)
# =========================
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "develop_1_lic")

# TecnoSpeed (consulta /api/v1/pix/{id})
TECNOSPEED_BASE = os.getenv("TECNOSPEED_BASE", "https://pix.tecnospeed.com.br")

# Webhook auth (se vazio, não valida)
WEBHOOK_AUTH = os.getenv("WEBHOOK_AUTH", "")

# PlugzAPI (WhatsApp)
PLUGZ_API_URL = os.getenv(
    "PLUGZ_API_URL",
    "https://api.plugzapi.com.br/instances/3EB1B4D6B8DE105D283A26D356DD90A9/token/4CD2BC99B9D4070109BC16EA/send-text"
)
PLUGZ_CLIENT_TOKEN = os.getenv("PLUGZ_CLIENT_TOKEN", "Fc0dd5429e2674e2e9cea2c0b5b29d000S")

# =========================
# Tabelas
# =========================
TBL_COBRANCAS = f"{DB_NAME}.pix_cobrancas_geradas"
TBL_RECEBIDOS = f"{DB_NAME}.pix_recebidos"
TBL_DADOSPIX = f"{DB_NAME}.dadospix"
TBL_EVENTOS = f"{DB_NAME}.pix_webhook_eventos"
TBL_AUTENTICACAO = f"{DB_NAME}.autenticacao"


# =========================
# Utils
# =========================
def safe_json(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"


def now_str():
    return datetime.now(TZ_BR).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def parse_iso_dt_to_br(s):
    """
    Converte ISO 8601 (ex.: 2026-02-11T16:37:03.112Z) para DATETIME local BR (-03:00)
    Retorna 'YYYY-MM-DD HH:MM:SS' (sem tz) ou None.
    """
    if not s:
        return None
    try:
        s2 = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_br = dt.astimezone(TZ_BR).replace(tzinfo=None)
        return dt_br.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def digits(s):
    return re.sub(r"\D+", "", str(s or ""))


def money_br(v):
    try:
        return f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v or "")


def fmt_dt_br(mysql_dt_str):
    """Recebe 'YYYY-MM-DD HH:MM:SS' e devolve 'DD/MM/YYYY HH:MM'."""
    if not mysql_dt_str:
        return ""
    try:
        dt = datetime.strptime(mysql_dt_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(mysql_dt_str)


# =========================
# DB
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


# =========================
# Vinculo (pix_cobrancas_geradas)
# =========================
def buscar_vinculo_por_pix(cursor, pix_id: str) -> dict:
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
    return row or {"id_cobrancas": None, "codigoparasistema": None, "codcadastro": None, "pedidovendaid": None}


# =========================
# Token company (dadospix)
# =========================
def buscar_dadospix(cursor, codigoparasistema, codcadastro) -> dict:
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
          ( %s IS NULL OR codigoparasistema = %s )
          AND
          ( %s IS NULL OR codcadastro = %s )
        ORDER BY iddadospix DESC
        LIMIT 1
        """,
        (codigoparasistema, codigoparasistema, codcadastro, codcadastro),
    )
    return cursor.fetchone() or {}


def token_expirado(expires_at) -> bool:
    if not expires_at:
        return True
    try:
        if isinstance(expires_at, str):
            dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        else:
            dt = expires_at
        # se vier sem tz, assume BR
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ_BR)
        return (dt - datetime.now(dt.tzinfo)).total_seconds() < 120
    except Exception:
        return True


def renovar_token_company(client_id: str, client_secret: str) -> dict:
    """
    POST /oauth2/token
    Body: grant_type=client_credentials & role=company
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

    r = requests.post(url, headers=headers, data=data, timeout=25)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Falha ao renovar token ({r.status_code}): {r.text}")

    j = r.json() or {}
    access_token = (j.get("access_token") or "").strip()
    expires_in = int(j.get("expires_in") or 3600)

    if not access_token:
        raise RuntimeError(f"Resposta sem access_token: {r.text}")

    expires_dt = (datetime.now(TZ_BR) + timedelta(seconds=expires_in)).replace(tzinfo=None)

    return {
        "access_token": access_token,
        "expires_at": expires_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "expires_in": expires_in,
    }


def garantir_token_company(cursor, codigoparasistema, codcadastro) -> str:
    dp = buscar_dadospix(cursor, codigoparasistema, codcadastro)
    if not dp:
        raise RuntimeError("Não encontrei registro em dadospix para esse vínculo.")

    token = (dp.get("token_company") or "").strip()
    expires_at = dp.get("token_company_expires_at")
    client_id = (dp.get("tecnospeed_client_id") or "").strip()
    client_secret = (dp.get("tecnospeed_client_secret") or "").strip()
    iddadospix = dp.get("iddadospix")

    if (not token) or token_expirado(expires_at):
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
        print(f"[INFO] Token company renovado (iddadospix={iddadospix}) expira em {novo['expires_at']}")

    return token


# =========================
# TecnoSpeed: Consultar PIX por ID
# =========================
def tecnospeed_consultar_pix_por_id(pix_id: str, token_company: str) -> dict:
    url = f"{TECNOSPEED_BASE}/api/v1/pix/{pix_id}"
    headers = {"Authorization": f"Bearer {token_company}", "Accept": "application/json"}
    r = requests.get(url, headers=headers, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Consulta PIX por ID falhou ({r.status_code}): {r.text}")
    data = r.json() or {}
    return data if isinstance(data, dict) else {}


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


def upsert_pix_recebido(cursor, pix_full: dict, vinculo: dict):
    """
    Regras:
    - Sempre INSERE com pago=0
    - Em UPDATE: NÃO ALTERA o campo pago (outro robô cuida disso)
    Retorna: previous_payment_date (pra decidir envio de WhatsApp sem duplicar)
    """
    pix_id = str(pix_full.get("id") or "")
    surrogate = str(pix_full.get("surrogateKey") or "")
    status = str(pix_full.get("status") or "")
    amount = pix_full.get("amount")
    payment_date = parse_iso_dt_to_br(pix_full.get("paymentDate"))
    payer_doc = str(pix_full.get("payerCpfCnpj") or "")
    payer_name = str(pix_full.get("payerName") or "")
    emv = str(pix_full.get("emv") or "")
    created_at_api = parse_iso_dt_to_br(pix_full.get("createdAt"))

    codigoparasistema = vinculo.get("codigoparasistema")
    codcadastro = vinculo.get("codcadastro")
    id_cobrancas = vinculo.get("id_cobrancas")

    cursor.execute(
        f"SELECT id_recebido, payment_date FROM {TBL_RECEBIDOS} WHERE pix_id=%s LIMIT 1",
        (pix_id,),
    )
    exists = cursor.fetchone()
    prev_payment_date = (exists.get("payment_date") if exists else None)

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
              codigoparasistema=%s,
              codcadastro=%s,
              id_cobrancas=%s,
              json_completo=%s
            WHERE pix_id=%s
            """,
            (
                surrogate, status, amount, payment_date, payer_doc, payer_name, emv, created_at_api,
                now_str(), codigoparasistema, codcadastro, id_cobrancas, safe_json(pix_full), pix_id
            ),
        )
    else:
        cursor.execute(
            f"""
            INSERT INTO {TBL_RECEBIDOS}
              (pix_id, surrogate_key, status, amount, payment_date, payer_cpf_cnpj, payer_name, emv,
               created_at_api, recebido_em, codigoparasistema, codcadastro, id_cobrancas, pago, json_completo)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,
               %s,%s,%s,%s,%s,0,%s)
            """,
            (
                pix_id, surrogate, status, amount, payment_date, payer_doc, payer_name, emv,
                created_at_api, now_str(), codigoparasistema, codcadastro, id_cobrancas, safe_json(pix_full)
            ),
        )

    return {"previous_payment_date": prev_payment_date, "payment_date": payment_date}


# =========================
# WhatsApp
# =========================
def format_br_phone(ddd, fone):
    d = digits(ddd)
    n = digits(fone)
    if not d or not n:
        return None
    return f"55{d}{n}"


def enviar_whatsapp(phone_e164: str, message: str) -> bool:
    payload = {"phone": phone_e164, "message": message}
    headers = {"Content-Type": "application/json", "Client-Token": PLUGZ_CLIENT_TOKEN}

    try:
        resp = requests.post(PLUGZ_API_URL, headers=headers, json=payload, timeout=25)
        print(f"✅ Mensagem enviada ao WhatsApp {phone_e164}. Status: {resp.status_code}")
        print("📟 Resposta da PlugzAPI:", resp.text)
        return resp.status_code in (200, 201)
    except Exception as e:
        print(f"❌ Erro ao enviar WhatsApp ({phone_e164}): {repr(e)}")
        return False


def obter_schema_por_codigoempresa(cursor, codigoempresa: int) -> str:
    cursor.execute(
        f"""
        SELECT ESQUEMA
        FROM {TBL_AUTENTICACAO}
        WHERE CODIGOEMPRESA = %s
        LIMIT 1
        """,
        (codigoempresa,),
    )
    row = cursor.fetchone()
    return (row.get("ESQUEMA") if row else "") or ""


def obter_cliente_empresa_e_telefones(cursor, schema: str, codcadastro: int):
    cursor.execute(
        f"""
        SELECT
          razaosocial,
          ddd1, fone1,
          ddd2, fone2,
          ddd3, fone3,
          ddd4, fone4,
          ddd5, fone5
        FROM `{schema}`.cadastro
        WHERE codcadastro = %s
        LIMIT 1
        """,
        (codcadastro,),
    )
    c = cursor.fetchone() or {}
    nome = c.get("razaosocial") or ""

    tels = []
    for i in range(1, 6):
        t = format_br_phone(c.get(f"ddd{i}"), c.get(f"fone{i}"))
        if t:
            tels.append(t)

    # remove duplicados
    out, seen = [], set()
    for t in tels:
        if t not in seen:
            out.append(t)
            seen.add(t)

    return nome, out


def obter_codcadastro_cliente_final(cursor, schema: str, pedidovendaid: int):
    cursor.execute(
        f"""
        SELECT codcadastro
        FROM `{schema}`.pedidovenda
        WHERE pedidovendaid = %s
        LIMIT 1
        """,
        (pedidovendaid,),
    )
    row = cursor.fetchone()
    return row.get("codcadastro") if row else None


def obter_nome_por_codcadastro(cursor, schema: str, codcadastro: int) -> str:
    if not codcadastro:
        return ""
    cursor.execute(
        f"""
        SELECT razaosocial
        FROM `{schema}`.cadastro
        WHERE codcadastro=%s
        LIMIT 1
        """,
        (codcadastro,),
    )
    r = cursor.fetchone() or {}
    return r.get("razaosocial") or ""


def montar_mensagem(nome_cliente_empresa: str, numero_pedido: str, nome_cliente_final: str, valor, data_mysql: str):
    return (
        f"Olá, {nome_cliente_empresa}! ✅\n"
        f"O pagamento do pedido nº {numero_pedido}, referente ao cliente {nome_cliente_final}, foi confirmado com sucesso.\n\n"
        f"💰 Valor: R$ {money_br(valor)}\n"
        f"📅 Data: {fmt_dt_br(data_mysql)}"
    )


# =========================
# Routes
# =========================
@app.get("/")
def home():
    return jsonify({"service": "pix-webhook", "status": "ok"}), 200


@app.post("/webhook/pix-pago")
def webhook_pix():
    """
    - Recebe PIX_SUCCESSFUL {event,id}
    - Salva SEMPRE em pix_webhook_eventos
    - Consulta GET /api/v1/pix/{id}
    - Se status == LIQUIDATED e paymentDate existe:
        - upsert pix_recebidos (sem tocar em pago; insert pago=0)
        - envia WhatsApp APENAS se antes payment_date era NULL e agora não é
    """
    try:
        auth = request.headers.get("Authorization", "")
        if WEBHOOK_AUTH and auth != WEBHOOK_AUTH:
            return jsonify({"error": "Unauthorized"}), 401

        payload = request.get_json(silent=True) or {}
        event_name = (payload.get("event") or payload.get("type") or "").strip()
        pix_id = (payload.get("id") or payload.get("pix_id") or "").strip()

        if not pix_id:
            return jsonify({"error": "pix_id ausente no payload"}), 400

        print("\n📨 Webhook recebido:")
        print(json.dumps(payload, ensure_ascii=False))

        headers_dict = {k: v for k, v in request.headers.items()}

        conn = db_conn()
        try:
            with conn.cursor() as cursor:
                inserir_evento(cursor, event_name, pix_id, headers_dict, payload)

                if event_name.upper() == "PIX_SUCCESSFUL":
                    vinculo = buscar_vinculo_por_pix(cursor, pix_id)
                    if not vinculo.get("codigoparasistema"):
                        print(f"[WARN] Sem vínculo em pix_cobrancas_geradas para pix_id={pix_id}.")
                        conn.commit()
                        return jsonify({"ok": True, "warn": "Sem vínculo"}), 200

                    token_company = garantir_token_company(
                        cursor,
                        vinculo.get("codigoparasistema"),
                        vinculo.get("codcadastro"),
                    )

                    pix_full = tecnospeed_consultar_pix_por_id(pix_id, token_company)

                    print("[INFO] Retorno TecnoSpeed /api/v1/pix/{id}:")
                    print(json.dumps(pix_full, ensure_ascii=False))

                    status_pix = str(pix_full.get("status") or "").upper().strip()
                    payment_date_br = parse_iso_dt_to_br(pix_full.get("paymentDate"))

                    # sempre salva/atualiza no recebidos (sem mexer no pago)
                    info = upsert_pix_recebido(cursor, pix_full, vinculo)

                    # manda WhatsApp apenas quando realmente liquidado e acabou de ganhar payment_date
                    prev_pd = info.get("previous_payment_date")
                    now_pd = info.get("payment_date")

                    if status_pix == "LIQUIDATED" and now_pd and (not prev_pd):
                        schema = obter_schema_por_codigoempresa(cursor, vinculo.get("codigoparasistema"))
                        schema = (schema or "").strip().lower()

                        nome_empresa, telefones = obter_cliente_empresa_e_telefones(
                            cursor, schema, vinculo.get("codcadastro")
                        )

                        pedidovendaid = vinculo.get("pedidovendaid")
                        cod_final = obter_codcadastro_cliente_final(cursor, schema, pedidovendaid)
                        nome_final = obter_nome_por_codcadastro(cursor, schema, cod_final)

                        valor = pix_full.get("amount")

                        msg = montar_mensagem(
                            nome_cliente_empresa=nome_empresa or "Cliente",
                            numero_pedido=str(pedidovendaid or ""),
                            nome_cliente_final=nome_final or "Cliente",
                            valor=valor,
                            data_mysql=payment_date_br or "",
                        )

                        if telefones:
                            for fone in telefones:
                                enviar_whatsapp(fone, msg)
                        else:
                            print(f"[WARN] Nenhum telefone encontrado em {schema}.cadastro (codcadastro={vinculo.get('codcadastro')}).")
                    else:
                        print(f"[INFO] Não envia WhatsApp (status={status_pix}, paymentDate={payment_date_br}, prev_payment_date={prev_pd}).")

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
    pix_id = request.args.get("pix_id", "").strip()

    conn = db_conn()
    try:
        with conn.cursor() as cursor:
            if pix_id:
                cursor.execute(
                    f"SELECT * FROM {TBL_EVENTOS} WHERE pix_id=%s ORDER BY id_evento DESC LIMIT 50",
                    (pix_id,),
                )
                eventos = cursor.fetchall()

                cursor.execute(
                    f"SELECT * FROM {TBL_RECEBIDOS} WHERE pix_id=%s LIMIT 1",
                    (pix_id,),
                )
                recebidos = cursor.fetchone()
            else:
                cursor.execute(f"SELECT * FROM {TBL_EVENTOS} ORDER BY id_evento DESC LIMIT 50")
                eventos = cursor.fetchall()

                cursor.execute(f"SELECT * FROM {TBL_RECEBIDOS} ORDER BY id_recebido DESC LIMIT 50")
                recebidos = cursor.fetchall()

        html = f"""
        <html>
          <head><meta charset="utf-8"><title>PIX Monitor</title></head>
          <body style="font-family: Arial; margin: 20px;">
            <h2>PIX Monitor (Railway)</h2>

            <form method="get" action="/ui" style="margin-bottom: 14px;">
              <label>Buscar por PIX ID:</label>
              <input name="pix_id" value="{pix_id}" style="width:520px;padding:6px;" />
              <button type="submit" style="padding:6px 10px;">Buscar</button>
              <a href="/ui" style="margin-left:10px;">Limpar</a>
            </form>

            <h3>Últimos Eventos (pix_webhook_eventos)</h3>
            <pre style="background:#f6f6f6;padding:10px;border-radius:8px;overflow:auto;max-height:360px;">{json.dumps(eventos, ensure_ascii=False, indent=2)}</pre>

            <h3>PIX Recebidos (pix_recebidos)</h3>
            <pre style="background:#f6f6f6;padding:10px;border-radius:8px;overflow:auto;max-height:360px;">{json.dumps(recebidos, ensure_ascii=False, indent=2)}</pre>
          </body>
        </html>
        """
        return Response(html, mimetype="text/html")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)