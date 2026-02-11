import os
import json
import pymysql
import requests
from datetime import datetime
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

# Token TecnoSpeed (Company)
TECNOSPEED_BASE = os.getenv("TECNOSPEED_BASE", "https://pix.tecnospeed.com.br")
TECNOSPEED_TOKEN = os.getenv("TECNOSPEED_TOKEN", "")  # Bearer token (access_token)

# Webhook auth (o mesmo que você cadastrou na TecnoSpeed)
WEBHOOK_AUTH = os.getenv("WEBHOOK_AUTH", "Basic dev854850")

# Tabelas
TBL_EVENTOS = f"{DB_NAME}.pix_webhook_eventos"
TBL_RECEBIDOS = f"{DB_NAME}.pix_recebidos"
TBL_COBRANCAS = f"{DB_NAME}.pix_cobrancas_geradas"


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


def now_utc_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def safe_json(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"


def tecnospeed_consultar_pix(pix_id: str) -> dict:
    """
    Consulta PIX por id e retorna o primeiro item (dict) se existir.
    Doc: /api/v1/pix/query
    """
    if not TECNOSPEED_TOKEN:
        raise RuntimeError("TECNOSPEED_TOKEN não configurado nas variáveis de ambiente.")

    url = f"{TECNOSPEED_BASE}/api/v1/pix/query"
    headers = {"Authorization": f"Bearer {TECNOSPEED_TOKEN}"}
    params = {"id": pix_id}

    r = requests.get(url, headers=headers, params=params, timeout=20)

    # Ex.: 200 com lista
    if r.status_code != 200:
        raise RuntimeError(f"Consulta PIX falhou ({r.status_code}): {r.text}")

    data = r.json()

    # a API pode retornar lista direto ou objeto com results
    if isinstance(data, list):
        return data[0] if data else {}
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        return data["results"][0] if data["results"] else {}

    # fallback
    return {}


def buscar_vinculo_cobranca(cursor, pix_id: str) -> dict:
    """
    Encontra id_cobrancas, codigoparasistema, codcadastro em pix_cobrancas_geradas pelo pix_id.
    """
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


def inserir_evento(cursor, event_name: str, pix_id: str, headers_json: dict, json_completo: dict):
    """
    Salva SEMPRE qualquer webhook recebido (auditoria).
    """
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
            now_utc_str(),
        ),
    )


def upsert_pix_recebido(cursor, pix: dict, vinculo: dict):
    """
    Insere na pix_recebidos somente para eventos de pagamento.
    Se já existir pix_id, atualiza os campos principais.
    """
    pix_id = str(pix.get("id") or "")
    surrogate = str(pix.get("surrogateKey") or "")
    status = str(pix.get("status") or "")
    amount = pix.get("amount", None)
    payment_date = pix.get("paymentDate", None)
    payer_doc = str(pix.get("payerCpfCnpj") or "")
    payer_name = str(pix.get("payerName") or "")
    emv = str(pix.get("emv") or "")
    created_at = pix.get("createdAt", None)

    # Seus vínculos
    codigoparasistema = vinculo.get("codigoparasistema")
    codcadastro = vinculo.get("codcadastro")
    id_cobrancas = vinculo.get("id_cobrancas")

    # Idempotência por pix_id
    cursor.execute(
        f"""
        SELECT id
        FROM {TBL_RECEBIDOS}
        WHERE pix_id = %s
        LIMIT 1
        """,
        (pix_id,),
    )
    exists = cursor.fetchone()

    if exists:
        cursor.execute(
            f"""
            UPDATE {TBL_RECEBIDOS}
            SET
              surrogate_key = %s,
              status = %s,
              amount = %s,
              payment_date = %s,
              payer_cpf_cnpj = %s,
              payer_name = %s,
              emv = %s,
              created_at = %s,
              recebido_em = %s,
              json_completo = %s,
              codigoparasistema = COALESCE(%s, codigoparasistema),
              codcadastro = COALESCE(%s, codcadastro),
              id_cobrancas = COALESCE(%s, id_cobrancas)
            WHERE pix_id = %s
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
                now_utc_str(),
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
              (%s, %s, %s, %s, %s, %s, %s, %s, %s,
               %s, %s, %s, %s, %s, 0)
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
                now_utc_str(),
                safe_json(pix),
                codigoparasistema,
                codcadastro,
                id_cobrancas,
            ),
        )


@app.get("/")
def home():
    return jsonify({"service": "pix-recebidos", "status": "ok"}), 200


@app.post("/webhook/pix-pago")
def webhook_pix():
    try:
        # 1) valida header Authorization
        auth = request.headers.get("Authorization", "")
        if WEBHOOK_AUTH and auth != WEBHOOK_AUTH:
            return jsonify({"error": "Unauthorized"}), 401

        # 2) pega JSON
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

                # 4) se for evento que indica pagamento, consulta dados completos e salva em pix_recebidos
                # TecnoSpeed: pagamento concluído costuma chegar como PIX_SUCCESSFUL
                if str(event_name).upper() in ("PIX_SUCCESSFUL", "PIX_PAID"):
                    pix_full = tecnospeed_consultar_pix(pix_id)

                    # se a consulta ainda não trouxe dados (raro, mas pode acontecer), não quebra
                    if pix_full:
                        vinculo = buscar_vinculo_cobranca(cursor, pix_id)
                        upsert_pix_recebido(cursor, pix_full, vinculo)

                conn.commit()

        finally:
            try:
                conn.close()
            except Exception:
                pass

        return jsonify({"ok": True}), 200

    except Exception as e:
        # Se der erro, você vai ver no log do Railway
        print("ERRO WEBHOOK:", repr(e))
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/ui")
def ui():
    """
    UI simples no Railway para acompanhar.
    /ui?pix_id=...
    """
    pix_id = request.args.get("pix_id", "").strip()

    conn = db_conn()
    try:
        with conn.cursor() as cursor:
            if pix_id:
                cursor.execute(
                    f"SELECT * FROM {TBL_EVENTOS} WHERE pix_id=%s ORDER BY id_evento DESC LIMIT 30",
                    (pix_id,),
                )
                eventos = cursor.fetchall()

                cursor.execute(
                    f"SELECT * FROM {TBL_RECEBIDOS} WHERE pix_id=%s LIMIT 1",
                    (pix_id,),
                )
                recebido = cursor.fetchone()
            else:
                cursor.execute(
                    f"SELECT * FROM {TBL_EVENTOS} ORDER BY id_evento DESC LIMIT 30"
                )
                eventos = cursor.fetchall()

                cursor.execute(
                    f"SELECT * FROM {TBL_RECEBIDOS} ORDER BY id DESC LIMIT 30"
                )
                recebido = cursor.fetchall()

        html = f"""
        <html>
          <head><meta charset="utf-8"><title>PIX Monitor</title></head>
          <body style="font-family: Arial; margin: 20px;">
            <h2>PIX Monitor (Railway)</h2>

            <form method="get" action="/ui" style="margin-bottom: 14px;">
              <label>Buscar por PIX ID:</label>
              <input name="pix_id" value="{pix_id}" style="width:420px;padding:6px;" />
              <button type="submit" style="padding:6px 10px;">Buscar</button>
              <a href="/ui" style="margin-left:10px;">Limpar</a>
            </form>

            <h3>Últimos Eventos (pix_webhook_eventos)</h3>
            <pre style="background:#f6f6f6;padding:10px;border-radius:8px;overflow:auto;max-height:360px;">
{json.dumps(eventos, ensure_ascii=False, indent=2)}
            </pre>

            <h3>Recebidos (pix_recebidos)</h3>
            <pre style="background:#f6f6f6;padding:10px;border-radius:8px;overflow:auto;max-height:360px;">
{json.dumps(recebido, ensure_ascii=False, indent=2)}
            </pre>

            <p><b>Dica:</b> se aparecer PIX_SUCCESSFUL aqui mas não tiver pix_recebidos vinculado,
            provavelmente a cobrança ainda não estava salva em pix_cobrancas_geradas no momento do webhook.</p>
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
    # Railway usa PORT automaticamente
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)