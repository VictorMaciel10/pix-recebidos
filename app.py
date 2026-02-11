import os
import json
import base64
import pymysql
import requests
from datetime import datetime, timezone, timedelta
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

TECNOSPEED_BASE = os.getenv("TECNOSPEED_BASE", "https://pix.tecnospeed.com.br")

# Header que você cadastrou na TecnoSpeed (EXATAMENTE)
WEBHOOK_AUTH = os.getenv("WEBHOOK_AUTH", "Basic dev854850")

# Tabelas (fixas)
TBL_COBRANCAS = f"{DB_NAME}.pix_cobrancas_geradas"
TBL_RECEBIDOS = f"{DB_NAME}.pix_recebidos"
TBL_DADOSPIX = f"{DB_NAME}.dadospix"
TBL_EVENTOS = f"{DB_NAME}.pix_webhook_eventos"


# =========================
# Helpers
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


def now_utc_naive_str():
    return datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def safe_json(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"


def parse_iso_dt(s: str):
    if not s:
        return None
    try:
        s2 = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        return dt.astimezone(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def get_ip_origem():
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or ""


# =========================
# Vínculo: pix_id -> cobrança/empresa
# =========================
def buscar_vinculo_por_pix(cursor, pix_id: str) -> dict:
    cursor.execute(
        f"""
        SELECT
          id_cobrancas,
          codigoparasistema,
          codcadastro
        FROM {TBL_COBRANCAS}
        WHERE pix_id = %s
        LIMIT 1
        """,
        (pix_id,),
    )
    row = cursor.fetchone()
    return row or {"id_cobrancas": None, "codigoparasistema": None, "codcadastro": None}


# =========================
# dadospix -> token_company (renova se precisar)
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
    if not expires_at:
        return True
    try:
        if isinstance(expires_at, str):
            dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        else:
            dt = expires_at
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (dt - datetime.now(timezone.utc)).total_seconds() < 120
    except Exception:
        return True


def renovar_token_company(client_id: str, client_secret: str) -> dict:
    """
    POST https://pix.tecnospeed.com.br/oauth2/token
    Headers:
      Content-Type: application/x-www-form-urlencoded
      Authorization: Basic base64(client_id:client_secret)
    Body:
      grant_type=client_credentials
      role=company
    """
    if not client_id or not client_secret:
        raise RuntimeError("client_id/client_secret ausentes no dadospix")

    url = f"{TECNOSPEED_BASE}/oauth2/token"

    raw = f"{client_id}:{client_secret}".encode("utf-8")
    b64 = base64.b64encode(raw).decode("utf-8")

    headers = {
        "Authorization": f"Basic {b64}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }

    data = {
        "grant_type": "client_credentials",
        "role": "company",
    }

    r = requests.post(url, headers=headers, data=data, timeout=20)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Falha ao renovar token ({r.status_code}): {r.text}")

    j = r.json() or {}
    access_token = j.get("access_token")
    expires_in = int(j.get("expires_in") or 3600)

    if not access_token:
        raise RuntimeError(f"Resposta sem access_token: {r.text}")

    expires_dt = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).replace(tzinfo=None)

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

    return token


# =========================
# TecnoSpeed: Consultar por ID Pix
# GET /api/v1/pix/{id}
# =========================
def tecnospeed_consultar_pix_por_id(pix_id: str, token_company: str) -> dict:
    url = f"{TECNOSPEED_BASE}/api/v1/pix/{pix_id}"
    headers = {"Authorization": f"Bearer {token_company}", "Accept": "application/json"}

    r = requests.get(url, headers=headers, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Consulta por ID falhou ({r.status_code}): {r.text}")

    data = r.json()
    return data if isinstance(data, dict) else {}


# =========================
# Extrair campos do retorno
# =========================
def extrair_payer_doc_name(pix: dict):
    """
    Tenta achar payerCpfCnpj / payerName em diferentes lugares possíveis.
    """
    # 1) direto
    doc = pix.get("payerCpfCnpj") or pix.get("payerCpfCnpj")
    name = pix.get("payerName")

    # 2) effectivePayer (muito comum em APIs PIX)
    eff = pix.get("effectivePayer") or {}
    if not doc:
        doc = eff.get("document") or eff.get("cpfCnpj") or eff.get("cpf_cnpj")
    if not name:
        name = eff.get("name") or eff.get("nome")

    # 3) payments[0]
    pays = pix.get("payments") if isinstance(pix.get("payments"), list) else []
    if pays:
        p0 = pays[0] if isinstance(pays[0], dict) else {}
        if not doc:
            doc = p0.get("payerCpfCnpj") or p0.get("cpfCnpj") or p0.get("document")
        if not name:
            name = p0.get("payerName") or p0.get("name")

    return (str(doc or "").strip(), str(name or "").strip())


def extrair_payment_date(pix: dict):
    """
    Usa paymentDate direto, senão tenta payments[0].paidAt / paymentDate.
    """
    dt = pix.get("paymentDate")
    if dt:
        return parse_iso_dt(dt)

    pays = pix.get("payments") if isinstance(pix.get("payments"), list) else []
    if pays:
        p0 = pays[0] if isinstance(pays[0], dict) else {}
        return parse_iso_dt(p0.get("paidAt") or p0.get("paymentDate"))

    return None


# =========================
# Inserts
# =========================
def inserir_evento(cursor, event_name: str, pix_id: str, json_payload: dict):
    """
    Grava SEMPRE o que chegou (auditoria).
    Ajuste os nomes das colunas se sua pix_webhook_eventos for diferente.
    """
    cursor.execute(
        f"""
        INSERT INTO {TBL_EVENTOS}
          (event_name, pix_id, recebido_em, ip_origem, json_completo)
        VALUES
          (%s, %s, %s, %s, %s)
        """,
        (
            str(event_name or ""),
            str(pix_id or ""),
            now_utc_naive_str(),
            get_ip_origem(),
            safe_json(json_payload),
        ),
    )


def upsert_pix_recebido(cursor, pix_full: dict, vinculo: dict):
    """
    Preenche exatamente os campos da sua tabela pix_recebidos.
    """
    pix_id = str(pix_full.get("id") or "").strip()
    if not pix_id:
        return

    surrogate_key = str(pix_full.get("surrogateKey") or "").strip()
    status = str(pix_full.get("status") or "").strip()
    amount = pix_full.get("amount")
    emv = pix_full.get("emv") or ""
    created_at_api = parse_iso_dt(pix_full.get("createdAt"))
    payment_date = extrair_payment_date(pix_full)
    payer_doc, payer_name = extrair_payer_doc_name(pix_full)

    codigoparasistema = vinculo.get("codigoparasistema")
    codcadastro = vinculo.get("codcadastro")
    id_cobrancas = vinculo.get("id_cobrancas")

    json_completo = safe_json(pix_full)

    cursor.execute(
        f"SELECT id_recebido FROM {TBL_RECEBIDOS} WHERE pix_id=%s LIMIT 1",
        (pix_id,),
    )
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
              id_cobrancas=COALESCE(%s, id_cobrancas)
            WHERE pix_id=%s
            """,
            (
                surrogate_key,
                status,
                amount,
                payment_date,
                payer_doc,
                payer_name,
                emv,
                created_at_api,
                now_utc_naive_str(),
                json_completo,
                codigoparasistema,
                codcadastro,
                id_cobrancas,
                pix_id,
            ),
        )
    else:
        cursor.execute(
            f"""
            INSERT INTO {TBL_RECEBIDOS}
              (pix_id, surrogate_key, status, amount, payment_date, payer_cpf_cnpj, payer_name, emv, created_at_api,
               recebido_em, codigoparasistema, codcadastro, id_cobrancas, pago, json_completo)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s,
               %s,%s,%s,%s,0,%s)
            """,
            (
                pix_id,
                surrogate_key,
                status,
                amount,
                payment_date,
                payer_doc,
                payer_name,
                emv,
                created_at_api,
                now_utc_naive_str(),
                codigoparasistema,
                codcadastro,
                id_cobrancas,
                json_completo,
            ),
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
    - Loga no Railway o que chegou
    - Grava SEMPRE em pix_webhook_eventos
    - Se event for PIX_SUCCESSFUL ou PIX_PAID:
        consulta GET /api/v1/pix/{id}
        e salva em pix_recebidos
    """
    try:
        # 1) valida Authorization
        auth = request.headers.get("Authorization", "")
        if WEBHOOK_AUTH and auth != WEBHOOK_AUTH:
            print(f"[WEBHOOK] Unauthorized auth_recebido='{auth}' esperado='{WEBHOOK_AUTH}'")
            return jsonify({"error": "Unauthorized"}), 401

        # 2) payload
        payload = request.get_json(silent=True) or {}
        event_name = payload.get("event") or payload.get("type") or ""
        pix_id = payload.get("id") or payload.get("pix_id") or ""

        print(f"[WEBHOOK] recebido event={event_name} pix_id={pix_id} payload={payload}")

        if not pix_id:
            return jsonify({"error": "id (pix_id) ausente no payload"}), 400

        conn = db_conn()
        try:
            with conn.cursor() as cursor:
                # 3) sempre salva evento
                inserir_evento(cursor, event_name, pix_id, payload)

                # 4) se for pago -> consulta por ID e salva completo
                ev = str(event_name).upper().strip()
                if ev in ("PIX_SUCCESSFUL", "PIX_PAID"):
                    vinculo = buscar_vinculo_por_pix(cursor, pix_id)
                    token_company = garantir_token_company(
                        cursor,
                        vinculo.get("codigoparasistema"),
                        vinculo.get("codcadastro"),
                    )

                    pix_full = tecnospeed_consultar_pix_por_id(pix_id, token_company)

                    print(
                        f"[PIX FULL] pix_id={pix_id} status={pix_full.get('status')} amount={pix_full.get('amount')} "
                        f"paymentDate={pix_full.get('paymentDate')} createdAt={pix_full.get('createdAt')}"
                    )

                    upsert_pix_recebido(cursor, pix_full, vinculo)

                conn.commit()

        finally:
            try:
                conn.close()
            except Exception:
                pass

        return jsonify({"ok": True}), 200

    except Exception as e:
        print("ERRO WEBHOOK:", repr(e))
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/ui")
def ui():
    """
    UI simples:
    /ui -> últimos eventos e últimos recebidos
    /ui?pix_id=... -> filtra por pix_id
    """
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

            <h3>Eventos (pix_webhook_eventos)</h3>
            <pre style="background:#f6f6f6;padding:10px;border-radius:8px;overflow:auto;max-height:360px;">{json.dumps(eventos, ensure_ascii=False, indent=2)}</pre>

            <h3>Recebidos (pix_recebidos)</h3>
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
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)