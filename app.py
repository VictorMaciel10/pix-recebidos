import os
import json
import pymysql
from flask import Flask, request, jsonify
from dateutil import parser

app = Flask(__name__)

def iso_to_mysql_dt(s):
    if not s:
        return None
    try:
        dt = parser.isoparse(str(s))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def conectar_banco():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT", "3306")),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )

@app.route("/", methods=["GET"])
def health():
    return jsonify({"service": "pix-recebidos", "status": "ok"})

@app.route("/webhook/pix-pago", methods=["POST"])
def webhook_pix_pago():
    conn = None
    try:
        payload = request.get_json(silent=True)
        ifIP = request.headers.get("X-Forwarded-For", request.remote_addr)

        if not payload:
            return jsonify({"ok": False, "error": "JSON vazio/inválido"}), 400

        itens = payload if isinstance(payload, list) else [payload]

        salvos = 0
        ignorados = 0

        conn = conectar_banco()
        with conn.cursor() as cursor:
            for item in itens:
                pix_id = str(item.get("id") or "").strip()
                if not pix_id:
                    ignorados += 1
                    continue

                status = str(item.get("status") or "").upper().strip()
                payment_date_iso = item.get("paymentDate")

                # ✅ SALVA SOMENTE PAGOS:
                # Regra: tem paymentDate preenchido
                if not payment_date_iso:
                    ignorados += 1
                    continue

                payment_date = iso_to_mysql_dt(payment_date_iso)
                surrogate_key = item.get("surrogateKey")
                amount = item.get("amount")
                payer_cpf_cnpj = item.get("payerCpfCnpj")
                payer_name = item.get("payerName")
                emv = item.get("emv")
                created_at = iso_to_mysql_dt(item.get("createdAt"))

                cursor.execute("""
                    INSERT INTO develop_1_lic.pix_recebidos
                      (pix_id, surrogate_key, status, amount, payment_date,
                       payer_cpf_cnpj, payer_name, emv, created_at, json_completo)
                    VALUES
                      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      surrogate_key = VALUES(surrogate_key),
                      status        = VALUES(status),
                      amount        = VALUES(amount),
                      payment_date  = VALUES(payment_date),
                      payer_cpf_cnpj= VALUES(payer_cpf_cnpj),
                      payer_name    = VALUES(payer_name),
                      emv           = VALUES(emv),
                      created_at    = VALUES(created_at),
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
                    created_at,
                    json.dumps(item, ensure_ascii=False)
                ))

                salvos += 1

            conn.commit()

        return jsonify({"ok": True, "salvos": salvos, "ignorados": ignorados}), 200

    except Exception as e:
        if conn:
            try: conn.rollback()
            except: pass
        print("Erro webhook:", e)
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()