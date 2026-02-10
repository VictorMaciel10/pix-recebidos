import os
import json
import pymysql
from flask import Flask, request, jsonify
from datetime import datetime, timezone

app = Flask(__name__)

# ==========================
# CONFIG
# ==========================
WEBHOOK_AUTH = os.getenv("WEBHOOK_AUTH", "Basic dev854850")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "bddevelop1.mysql.database.azure.com"),
    "user": os.getenv("DB_USER", "bddevelop"),
    "password": os.getenv("DB_PASS", "E130581.rik"),
    "database": os.getenv("DB_NAME", "develop_1_lic"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": False,
}

def conectar_banco():
    return pymysql.connect(**DB_CONFIG)

def parse_iso_to_mysql_dt(iso_str):
    """
    Converte ISO 8601 (ex: 2021-08-25T00:19:23.248Z) para 'YYYY-MM-DD HH:MM:SS' (UTC).
    Retorna None se inválido.
    """
    if not iso_str:
        return None
    try:
        s = str(iso_str).strip()
        # trata Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        # garante timezone
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        # mantém UTC no banco
        dt_utc = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt_utc.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def pick_event_name(payload: dict):
    # a TecnoSpeed pode mandar o evento em campos diferentes dependendo do produto.
    # vamos tentar cobrir os mais comuns:
    for k in ["event", "tipoWH", "type", "eventName", "onEvent"]:
        v = payload.get(k)
        if v:
            return str(v)
    return None

def normalize_payment_obj(payload):
    """
    A TecnoSpeed pode mandar:
    - um array de objetos
    - um objeto direto
    - um wrapper { data: {...} } etc
    Aqui retornamos 1 objeto "principal" de pagamento quando existir.
    """
    if payload is None:
        return None

    # se vier lista
    if isinstance(payload, list) and len(payload) > 0 and isinstance(payload[0], dict):
        return payload[0]

    # se vier objeto
    if isinstance(payload, dict):
        # alguns wrappers comuns
        for key in ["data", "pix", "payment", "payload"]:
            if isinstance(payload.get(key), dict):
                return payload[key]
        return payload

    return None

def is_paid_event(event_name, status):
    e = (event_name or "").upper().strip()
    s = (status or "").upper().strip()
    # o que você quer considerar como pagamento confirmado:
    # - evento PIX_PAID
    # - status PAID (caso venha em outros eventos)
    return (e == "PIX_PAID") or (s == "PAID")

def fetch_cobranca_vinculo(cursor, pix_id):
    """
    Busca vínculo com a cobrança gerada no seu sistema (pix_cobrancas_geradas).
    """
    if not pix_id:
        return None

    cursor.execute("""
        SELECT
          id_cobrancas,
          codigoparasistema,
          codcadastro
        FROM develop_1_lic.pix_cobrancas_geradas
        WHERE pix_id = %s
        ORDER BY id_cobrancas DESC
        LIMIT 1
    """, (str(pix_id),))
    return cursor.fetchone()

def insert_log_event(cursor, event_name, obj, headers_json, json_completo, ip_origem):
    pix_id = obj.get("id")
    surrogate_key = obj.get("surrogateKey") or obj.get("surrogate_key")
    status = obj.get("status")
    amount = obj.get("amount")
    payer_cpf_cnpj = obj.get("payerCpfCnpj")
    payer_name = obj.get("payerName")
    emv = obj.get("emv")

    payment_date = parse_iso_to_mysql_dt(obj.get("paymentDate"))
    created_at_api = parse_iso_to_mysql_dt(obj.get("createdAt"))

    cursor.execute("""
        INSERT INTO develop_1_lic.pix_webhook_eventos
        (
          event_name, pix_id, surrogate_key, status, amount,
          payment_date, payer_cpf_cnpj, payer_name, emv, created_at_api,
          ip_origem, headers_json, json_completo
        )
        VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CAST(%s AS JSON),%s)
    """, (
        event_name,
        pix_id,
        surrogate_key,
        status,
        amount,
        payment_date,
        payer_cpf_cnpj,
        payer_name,
        emv,
        created_at_api,
        ip_origem,
        json.dumps(headers_json, ensure_ascii=False),
        json_completo
    ))

def upsert_pix_recebido_paid(cursor, obj, json_completo):
    """
    Salva/atualiza em pix_recebidos somente quando for PAID.
    E já tenta vincular à cobrança gerada.
    pago SEMPRE 0.
    """
    pix_id = str(obj.get("id") or "").strip()
    if not pix_id:
        return False, "pix_id vazio"

    surrogate_key = obj.get("surrogateKey") or obj.get("surrogate_key")
    status = obj.get("status")
    amount = obj.get("amount")
    payer_cpf_cnpj = obj.get("payerCpfCnpj")
    payer_name = obj.get("payerName")
    emv = obj.get("emv")

    payment_date = parse_iso_to_mysql_dt(obj.get("paymentDate"))
    created_at_api = parse_iso_to_mysql_dt(obj.get("createdAt"))

    vinc = fetch_cobranca_vinculo(cursor, pix_id) or {}
    id_cobrancas = vinc.get("id_cobrancas")
    codigoparasistema = vinc.get("codigoparasistema")
    codcadastro = vinc.get("codcadastro")

    cursor.execute("""
        INSERT INTO develop_1_lic.pix_recebidos
        (
          pix_id, surrogate_key, status, amount,
          payment_date, payer_cpf_cnpj, payer_name, emv, created_at_api,
          codigoparasistema, codcadastro, id_cobrancas,
          pago, json_completo
        )
        VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s)
        ON DUPLICATE KEY UPDATE
          surrogate_key = VALUES(surrogate_key),
          status = VALUES(status),
          amount = VALUES(amount),
          payment_date = VALUES(payment_date),
          payer_cpf_cnpj = VALUES(payer_cpf_cnpj),
          payer_name = VALUES(payer_name),
          emv = VALUES(emv),
          created_at_api = VALUES(created_at_api),
          codigoparasistema = COALESCE(VALUES(codigoparasistema), codigoparasistema),
          codcadastro = COALESCE(VALUES(codcadastro), codcadastro),
          id_cobrancas = COALESCE(VALUES(id_cobrancas), id_cobrancas),
          -- pago fica 0 sempre, então não atualiza pra 1 aqui
          json_completo = VALUES(json_completo)
    """, (
        pix_id,
        surrogate_key,
        status,
        amount,
        payment_date,
        payer_cpf_cnpj,
        payer_name,
        emv,
        created_at_api,
        codigoparasistema,
        codcadastro,
        id_cobrancas,
        json_completo
    ))
    return True, {"pix_id": pix_id, "id_cobrancas": id_cobrancas, "codigoparasistema": codigoparasistema, "codcadastro": codcadastro}

@app.get("/")
def health():
    return jsonify({"service": "pix-recebidos", "status": "ok"}), 200

@app.post("/webhook/pix-pago")
def webhook_pix_pago():
    # 1) valida auth do callback (seu segredo)
    auth = request.headers.get("Authorization", "")
    if auth != WEBHOOK_AUTH:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    # 2) lê payload
    payload = request.get_json(silent=True)
    if payload is None:
        return jsonify({"ok": False, "error": "invalid_json"}), 400

    # 3) prepara dados
    ip_origem = request.headers.get("X-Forwarded-For", request.remote_addr)
    headers_json = dict(request.headers)

    # grava json completo do jeito que chegou
    json_completo = json.dumps(payload, ensure_ascii=False)

    event_name = pick_event_name(payload)
    obj = normalize_payment_obj(payload) or {}

    status = obj.get("status")
    paid = is_paid_event(event_name, status)

    conn = None
    try:
        conn = conectar_banco()
        with conn.cursor() as cursor:
            # 4) LOG: sempre salva tudo
            insert_log_event(cursor, event_name, obj, headers_json, json_completo, ip_origem)

            # 5) CONSOLIDADO: só se for pagamento confirmado
            paid_info = None
            if paid:
                ok_paid, paid_info = upsert_pix_recebido_paid(cursor, obj, json_completo)
                if not ok_paid:
                    # Mesmo se falhar no consolidado, o log já foi salvo
                    pass

        conn.commit()

        return jsonify({
            "ok": True,
            "saved_log": True,
            "saved_paid": bool(paid),
            "event": event_name,
            "status": status,
        }), 200

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)