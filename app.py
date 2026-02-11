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

TECNOSPEED_BASE = os.getenv("TECNOSPEED_BASE", "https://pix.tecnospeed.com.br")

# Webhook auth (EXATAMENTE o que você cadastrou na TecnoSpeed)
WEBHOOK_AUTH = os.getenv("WEBHOOK_AUTH", "Basic dev854850")

# Tabelas (nomes fixos conforme você disse)
TBL_COBRANCAS = f"{DB_NAME}.pix_cobrancas_geradas"
TBL_RECEBIDOS = f"{DB_NAME}.pix_recebidos"
TBL_DADOSPIX = f"{DB_NAME}.dadospix"
TBL_EVENTOS = f"{DB_NAME}.pix_webhook_eventos"


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
    # grava como timestamp "local" do servidor (Railway) - mas ok pra auditoria
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_json(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"


def parse_iso_dt(s: str):
    """
    Converte '2021-08-25T00:19:23.248Z' em 'YYYY-MM-DD HH:MM:SS'
    Retorna None se vazio.
    """
    if not s:
        return None
    try:
        # normaliza Z
        s2 = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        # salva sem timezone (MySQL DATETIME)
        return dt.astimezone(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


# =========================
# TecnoSpeed Token (via dadospix)
# =========================
def buscar_vinculo_por_pix(cursor, pix_id: str) -> dict:
    """
    Encontra vínculo do PIX gerado no seu sistema.
    """
    cursor.execute(
        f"""
        SELECT
          id_cobrancas,
          codigoparasistema,
          codcadastro,
          tecnospeed_account_id
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
        "tecnospeed_account_id": None,
    }


def buscar_dadospix(cursor, codigoparasistema, codcadastro) -> dict:
    """
    Busca credenciais/token no dadospix.
    Ajuste o WHERE conforme sua regra real.
    Aqui eu uso (codigoparasistema, codcadastro) e pego o registro mais recente.
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
    expires_at pode vir como datetime do MySQL ou string.
    Considera expirado se faltam < 120s (margem).
    """
    if not expires_at:
        return True
    try:
        if isinstance(expires_at, str):
            dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        else:
            dt = expires_at
        # dt pode vir sem tz
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (dt - now).total_seconds() < 120
    except Exception:
        return True


def renovar_token_company(client_id: str, client_secret: str) -> dict:
    """
    Renova token via OAuth2 (Basic Auth).
    Endpoint correto é /oauth2/token (não /api/v1/oauth2/token).
    """
    if not client_id or not client_secret:
        raise RuntimeError("client_id/client_secret ausentes no dadospix")

    url = f"{TECNOSPEED_BASE}/oauth2/token"

    # Basic base64(client_id:client_secret)
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    b64 = base64.b64encode(raw).decode("utf-8")

    headers = {
        "Authorization": f"Basic {b64}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }

    # TecnoSpeed geralmente aceita grant_type=client_credentials
    data = {"grant_type": "client_credentials"}

    r = requests.post(url, headers=headers, data=data, timeout=20)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Falha ao renovar token ({r.status_code}): {r.text}")

    j = r.json() or {}
    access_token = j.get("access_token")
    expires_in = int(j.get("expires_in") or 3600)

    if not access_token:
        raise RuntimeError(f"Resposta sem access_token: {r.text}")

    # calcula expires_at (UTC)
    expires_at = datetime.now(timezone.utc).timestamp() + expires_in
    expires_dt = datetime.fromtimestamp(expires_at, tz=timezone.utc).replace(tzinfo=None)

    return {
        "access_token": access_token,
        "expires_at": expires_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "expires_in": expires_in,
    }


def garantir_token_company(cursor, codigoparasistema, codcadastro) -> str:
    """
    Retorna token_company válido (string).
    Se expirado, renova e salva em dadospix.
    """
    dp = buscar_dadospix(cursor, codigoparasistema, codcadastro)
    if not dp:
        raise RuntimeError("Não encontrei registro em dadospix para esse vínculo.")

    token = (dp.get("token_company") or "").strip()
    expires_at = dp.get("token_company_expires_at")
    client_id = (dp.get("tecnospeed_client_id") or "").strip()
    client_secret = (dp.get("tecnospeed_client_secret") or "").strip()
    iddadospix = dp.get("iddadospix")

    if not token or token_expirado(expires_at):
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
# TecnoSpeed: Consultar PIX
# =========================
def tecnospeed_consultar_pix(pix_id: str, token_company: str) -> dict:
    """
    Consulta PIX por ID
    GET /api/v1/pix/query?id=<pix_id>
    """
    url = f"{TECNOSPEED_BASE}/api/v1/pix/query"
    headers = {"Authorization": f"Bearer {token_company}", "Accept": "application/json"}
    params = {"id": pix_id}

    r = requests.get(url, headers=headers, params=params, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Consulta PIX falhou ({r.status_code}): {r.text}")

    data = r.json()

    # Pode vir como lista direta
    if isinstance(data, list):
        return data[0] if data else {}

    # Ou dict com results
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        return data["results"][0] if data["results"] else {}

    return {}


# =========================
# Inserts
# =========================
def inserir_evento(cursor, event_name: str, pix_id: str, headers_json: dict, json_completo: dict):
    # Ajuste aqui os nomes de colunas conforme seu schema REAL da pix_webhook_eventos
    cursor.execute(
        f"""
        INSERT INTO {TBL_EVENTOS}
          (event_name, pix_id, headers_json, json_completo, received_at)
        VALUES
          (%s, %s, %s, %s, %s)
        """,
        (
            str(event_name or ""),
            str(pix_id or ""),
            safe_json(headers_json),
            safe_json(json_completo),
            now_str(),
        ),
    )


def upsert_pix_recebido(cursor, pix: dict, vinculo: dict):
    """
    Salva na pix_recebidos (apenas quando for evento de pagamento confirmado)
    """
    pix_id = str(pix.get("id") or "")
    surrogate = str(pix.get("surrogateKey") or "")
    status = str(pix.get("status") or "")
    amount = pix.get("amount")
    payment_date = parse_iso_dt(pix.get("paymentDate"))
    payer_doc = str(pix.get("payerCpfCnpj") or pix.get("payerCpfCnpj") or "")
    payer_name = str(pix.get("payerName") or "")
    emv = str(pix.get("emv") or "")
    created_at = parse_iso_dt(pix.get("createdAt"))

    codigoparasistema = vinculo.get("codigoparasistema")
    codcadastro = vinculo.get("codcadastro")
    id_cobrancas = vinculo.get("id_cobrancas")

    # idempotência por pix_id
    cursor.execute(
        f"SELECT id FROM {TBL_RECEBIDOS} WHERE pix_id=%s LIMIT 1",
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
              created_at=%s,
              recebido_em=%s,
              json_completo=%s,
              codigoparasistema=COALESCE(%s, codigoparasistema),
              codcadastro=COALESCE(%s, codcadastro),
              id_cobrancas=COALESCE(%s, id_cobrancas)
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
                created_at,
                now_str(),
                safe_json(pix),
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
              (pix_id, surrogate_key, status, amount, payment_date, payer_cpf_cnpj, payer_name, emv, created_at,
               recebido_em, json_completo, codigoparasistema, codcadastro, id_cobrancas, pago)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s,
               %s,%s,%s,%s,%s,0)
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
                created_at,
                now_str(),
                safe_json(pix),
                codigoparasistema,
                codcadastro,
                id_cobrancas,
            ),
        )


# =========================
# Routes
# =========================
@app.get("/")
def home():
    return jsonify({"service": "pix-recebidos", "status": "ok"}), 200


@app.post("/webhook/pix-pago")
def webhook_pix():
    """
    Recebe QUALQUER evento do webhook da TecnoSpeed.
    - sempre salva em pix_webhook_eventos
    - se evento for "pago confirmado", consulta pix/query e salva em pix_recebidos
    """
    try:
        # 1) valida Authorization
        auth = request.headers.get("Authorization", "")
        if WEBHOOK_AUTH and auth != WEBHOOK_AUTH:
            return jsonify({"error": "Unauthorized"}), 401

        # 2) payload
        payload = request.get_json(silent=True) or {}
        event_name = payload.get("event") or payload.get("type") or ""
        pix_id = payload.get("id") or payload.get("pix_id") or ""

        if not pix_id:
            return jsonify({"error": "pix_id ausente no payload"}), 400

        headers_dict = {k: v for k, v in request.headers.items()}

        conn = db_conn()
        try:
            with conn.cursor() as cursor:
                # 3) salva evento SEMPRE
                inserir_evento(cursor, event_name, pix_id, headers_dict, payload)

                # 4) para eventos de pagamento confirmado -> consulta e salva detalhes
                ev = str(event_name).upper().strip()
                if ev in ("PIX_SUCCESSFUL", "PIX_PAID"):
                    vinculo = buscar_vinculo_por_pix(cursor, pix_id)

                    # token_company vem de dadospix (e renova se precisar)
                    token_company = garantir_token_company(
                        cursor,
                        vinculo.get("codigoparasistema"),
                        vinculo.get("codcadastro"),
                    )

                    pix_full = tecnospeed_consultar_pix(pix_id, token_company)

                    if pix_full:
                        upsert_pix_recebido(cursor, pix_full, vinculo)
                    else:
                        print(f"[WARN] Consulta pix/query retornou vazio para pix_id={pix_id}")

                conn.commit()

        finally:
            try:
                conn.close()
            except Exception:
                pass

        return jsonify({"ok": True}), 200

    except Exception as e:
        # Log no Railway
        print("ERRO WEBHOOK:", repr(e))
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/ui")
def ui():
    """
    UI simples para acompanhar no Railway:
    - /ui            -> últimos eventos + últimos recebidos
    - /ui?pix_id=... -> eventos desse pix + linha em recebidos
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
                cursor.execute(
                    f"SELECT * FROM {TBL_EVENTOS} ORDER BY id_evento DESC LIMIT 50"
                )
                eventos = cursor.fetchall()

                cursor.execute(
                    f"SELECT * FROM {TBL_RECEBIDOS} ORDER BY id DESC LIMIT 50"
                )
                recebidos = cursor.fetchall()

        html = f"""
        <html>
          <head>
            <meta charset="utf-8">
            <title>PIX Monitor</title>
          </head>
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

            <p><b>Nota:</b> Se aparecer PIX_SUCCESSFUL nos eventos, mas não inseriu em pix_recebidos,
            o motivo mais comum é: <b>não encontrou vínculo em pix_cobrancas_geradas</b> para esse pix_id,
            ou <b>token_company inválido/expirado</b> no dadospix (aqui já tentamos renovar).</p>
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