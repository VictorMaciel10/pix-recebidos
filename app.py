import os
import json
import base64
import pymysql
import requests
from datetime import datetime, timedelta, timezone
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

# EXATAMENTE o que você cadastrou na TecnoSpeed
WEBHOOK_AUTH = os.getenv("WEBHOOK_AUTH", "Basic dev854850")

# Tabelas (nomes fixos)
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
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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


# =========================
# Vinculo: pix_cobrancas_geradas
# =========================
def buscar_vinculo_por_pix(cursor, pix_id: str) -> dict:
    """
    Ajustado para o seu schema real: id_cobrancas, codigoparasistema, codcadastro
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


# =========================
# dadospix + token_company
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
    """
    considera expirado se faltar < 120s
    """
    if not expires_at:
        return True
    try:
        if isinstance(expires_at, str):
            # MySQL DATETIME -> "YYYY-MM-DD HH:MM:SS"
            dt = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
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
    body: grant_type=client_credentials & role=company
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
        "role": "company",   # <<< AQUI o ajuste que faltava (evita 422)
    }

    r = requests.post(url, headers=headers, data=data, timeout=25)
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
    A API pode exigir date/range. Vamos:
    1) tentar por id direto
    2) se der 400 pedindo date/range -> tenta range últimos 30 dias + id
    """
    url = f"{TECNOSPEED_BASE}/api/v1/pix/query"
    headers = {"Authorization": f"Bearer {token_company}", "Accept": "application/json"}

    # tentativa 1: só id
    r1 = requests.get(url, headers=headers, params={"id": pix_id}, timeout=25)
    if r1.status_code == 200:
        data = r1.json()
        if isinstance(data, list):
            return data[0] if data else {}
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            return data["results"][0] if data["results"] else {}
        return {}

    # fallback: se reclamar de date/range
    msg = (r1.text or "")
    if r1.status_code == 400 and ("Date or range of dates" in msg or "betweenDateStart" in msg or "date" in msg):
        end = datetime.now().date()
        start = end - timedelta(days=30)

        params = {
            "betweenDateStart": start.strftime("%Y-%m-%d"),
            "betweenDateEnd": end.strftime("%Y-%m-%d"),
            "queryType": "PAYMENT",   # geralmente o que faz sentido ao confirmar pagamento
            "id": pix_id,
            "limit": 1,
        }
        r2 = requests.get(url, headers=headers, params=params, timeout=25)
        if r2.status_code != 200:
            raise RuntimeError(f"Consulta PIX falhou ({r2.status_code}): {r2.text}")

        data = r2.json()
        if isinstance(data, list):
            return data[0] if data else {}
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            return data["results"][0] if data["results"] else {}
        return {}

    # qualquer outro erro
    raise RuntimeError(f"Consulta PIX falhou ({r1.status_code}): {r1.text}")


# =========================
# Inserts
# =========================
def inserir_evento(cursor, event_name: str, pix_id: str, headers_json: dict, json_completo: dict):
    cursor.execute(
        f"""
        INSERT INTO {TBL_EVENTOS}
          (event_name, pix_id, headers_json, json_completo, recebido_em)
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


def upsert_pix_recebido_minimo(cursor, pix_id: str, event_name: str, vinculo: dict, payload: dict):
    """
    Se não conseguir consultar a TecnoSpeed no momento, grava pelo menos o pix_id + status.
    Depois, quando consultar, ele atualiza.
    """
    cursor.execute(f"SELECT id_recebido FROM {TBL_RECEBIDOS} WHERE pix_id=%s LIMIT 1", (pix_id,))
    exists = cursor.fetchone()

    codigoparasistema = vinculo.get("codigoparasistema")
    codcadastro = vinculo.get("codcadastro")
    id_cobrancas = vinculo.get("id_cobrancas")

    if exists:
        cursor.execute(
            f"""
            UPDATE {TBL_RECEBIDOS}
            SET
              status=%s,
              recebido_em=%s,
              json_completo=%s,
              codigoparasistema=COALESCE(%s, codigoparasistema),
              codcadastro=COALESCE(%s, codcadastro),
              id_cobrancas=COALESCE(%s, id_cobrancas)
            WHERE pix_id=%s
            """,
            (str(event_name or ""), now_str(), safe_json(payload), codigoparasistema, codcadastro, id_cobrancas, pix_id),
        )
    else:
        cursor.execute(
            f"""
            INSERT INTO {TBL_RECEBIDOS}
              (pix_id, status, recebido_em, json_completo, codigoparasistema, codcadastro, id_cobrancas, pago)
            VALUES
              (%s, %s, %s, %s, %s, %s, %s, 0)
            """,
            (pix_id, str(event_name or ""), now_str(), safe_json(payload), codigoparasistema, codcadastro, id_cobrancas),
        )


def upsert_pix_recebido_completo(cursor, pix: dict, vinculo: dict):
    pix_id = str(pix.get("id") or "")
    surrogate = str(pix.get("surrogateKey") or "")
    status = str(pix.get("status") or "")
    amount = pix.get("amount")
    payment_date = parse_iso_dt(pix.get("paymentDate"))
    payer_doc = str(pix.get("payerCpfCnpj") or "")
    payer_name = str(pix.get("payerName") or "")
    emv = str(pix.get("emv") or "")
    created_at = parse_iso_dt(pix.get("createdAt"))

    codigoparasistema = vinculo.get("codigoparasistema")
    codcadastro = vinculo.get("codcadastro")
    id_cobrancas = vinculo.get("id_cobrancas")

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
              created_at=%s,
              recebido_em=%s,
              json_completo=%s,
              codigoparasistema=COALESCE(%s, codigoparasistema),
              codcadastro=COALESCE(%s, codcadastro),
              id_cobrancas=COALESCE(%s, id_cobrancas)
            WHERE pix_id=%s
            """,
            (
                surrogate, status, amount, payment_date,
                payer_doc, payer_name, emv, created_at,
                now_str(), safe_json(pix),
                codigoparasistema, codcadastro, id_cobrancas,
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
                pix_id, surrogate, status, amount, payment_date, payer_doc, payer_name, emv, created_at,
                now_str(), safe_json(pix), codigoparasistema, codcadastro, id_cobrancas
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
    Recebe QUALQUER evento TecnoSpeed.
    - sempre salva em pix_webhook_eventos
    - se evento for "pago", tenta consultar detalhes e salvar em pix_recebidos
    """
    try:
        auth = request.headers.get("Authorization", "")
        if WEBHOOK_AUTH and auth != WEBHOOK_AUTH:
            return jsonify({"error": "Unauthorized"}), 401

        payload = request.get_json(silent=True) or {}
        event_name = (payload.get("event") or payload.get("type") or "").strip()
        pix_id = (payload.get("id") or payload.get("pix_id") or "").strip()

        if not pix_id:
            return jsonify({"error": "id (pix_id) ausente no payload"}), 400

        headers_dict = {k: v for k, v in request.headers.items()}

        conn = db_conn()
        try:
            with conn.cursor() as cursor:
                # 1) auditoria sempre
                inserir_evento(cursor, event_name, pix_id, headers_dict, payload)

                ev = event_name.upper()
                if ev in ("PIX_SUCCESSFUL", "PIX_PAID"):
                    vinculo = buscar_vinculo_por_pix(cursor, pix_id)

                    # garante que pelo menos o "pago" fique rastreável em pix_recebidos
                    upsert_pix_recebido_minimo(cursor, pix_id, ev, vinculo, payload)

                    # tenta consultar o PIX completo
                    try:
                        token_company = garantir_token_company(
                            cursor,
                            vinculo.get("codigoparasistema"),
                            vinculo.get("codcadastro"),
                        )
                        pix_full = tecnospeed_consultar_pix(pix_id, token_company)
                        if pix_full:
                            upsert_pix_recebido_completo(cursor, pix_full, vinculo)
                    except Exception as e:
                        # não derruba, só loga
                        print(f"[WARN] Falha ao consultar PIX completo (pix_id={pix_id}): {repr(e)}")

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
    - /ui            -> últimos eventos + últimos recebidos
    - /ui?pix_id=... -> filtra por pix_id
    """
    pix_id = request.args.get("pix_id", "").strip()

    conn = db_conn()
    try:
        with conn.cursor() as cursor:
            if pix_id:
                cursor.execute(
                    f"SELECT * FROM {TBL_EVENTOS} WHERE pix_id=%s ORDER BY id_evento DESC LIMIT 80",
                    (pix_id,),
                )
                eventos = cursor.fetchall()

                cursor.execute(
                    f"SELECT * FROM {TBL_RECEBIDOS} WHERE pix_id=%s LIMIT 1",
                    (pix_id,),
                )
                recebidos = cursor.fetchone()
            else:
                cursor.execute(f"SELECT * FROM {TBL_EVENTOS} ORDER BY id_evento DESC LIMIT 80")
                eventos = cursor.fetchall()

                cursor.execute(f"SELECT * FROM {TBL_RECEBIDOS} ORDER BY id_recebido DESC LIMIT 80")
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

            <p><b>Obs:</b> Se vier PIX_SUCCESSFUL e não vier completo (payerName/amount/etc),
            você ainda vai ter a linha mínima em pix_recebidos. Depois, quando o token estiver ok e a consulta bater,
            ela atualiza com os dados completos.</p>
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