import os
import json
from datetime import datetime
from flask import Flask, request, jsonify
import pymysql

app = Flask(__name__)

# =========================
# ENV (Railway Variables)
# =========================
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "develop_1_lic")

# Segredo que você cadastrou na TecnoSpeed (Authorization header que ELES vão mandar pra você)
WEBHOOK_AUTH = os.getenv("WEBHOOK_AUTH", "Basic dev854850")  # pode deixar fixo ou setar no Railway

def conectar_banco():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        port=DB_PORT,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )

@app.route("/", methods=["GET"])
def health():
    return jsonify({"service": "pix-recebidos", "status": "ok"}), 200

# =========================
# WEBHOOK: Pix pago
# =========================
@app.route("/webhook/pix-pago", methods=["POST"])
def webhook_pix_pago():
    # 1) valida header Authorization (segurança)
    auth = request.headers.get("Authorization", "")
    if auth != WEBHOOK_AUTH:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    # 2) lê o JSON recebido (TecnoSpeed pode mandar lista)
    dados = request.get_json(silent=True)

    if not dados:
        return jsonify({"ok": False, "error": "json_vazio_ou_invalido"}), 400

    # normaliza: se vier objeto, transforma em lista
    if isinstance(dados, dict):
        dados = [dados]

    salvos = 0
    ignorados = 0
    erros = []

    conn = None
    try:
        conn = conectar_banco()
        with conn.cursor() as cursor:
            for item in dados:
                try:
                    pix_id = str(item.get("id") or "").strip()
                    if not pix_id:
                        ignorados += 1
                        continue

                    surrogate_key = str(item.get("surrogateKey") or "").strip()
                    status = str(item.get("status") or "").strip().upper()
                    amount = item.get("amount")
                    payment_date = item.get("paymentDate")
                    payer_cpf_cnpj = str(item.get("payerCpfCnpj") or "").strip()
                    payer_name = str(item.get("payerName") or "").strip()
                    emv = str(item.get("emv") or "").strip()
                    created_at = item.get("createdAt")

                    # 3) tenta vincular com a cobrança gerada
                    cursor.execute("""
                        SELECT id_cobrancas, codigoparasistema, codcadastro
                        FROM pix_cobrancas_geradas
                        WHERE pix_id = %s
                        LIMIT 1
                    """, (pix_id,))
                    cobranca = cursor.fetchone() or {}

                    id_cobrancas = cobranca.get("id_cobrancas")
                    codigoparasistema = cobranca.get("codigoparasistema")
                    codcadastro = cobranca.get("codcadastro")

                    # 4) grava em pix_recebidos (evita duplicar)
                    # ajuste aqui o nome da PK conforme sua tabela pix_recebidos
                    cursor.execute("""
                        INSERT INTO pix_recebidos
                        (pix_id, surrogate_key, status, amount, payment_date, payer_cpf_cnpj, payer_name, emv, created_at,
                         json_completo, codigoparasistema, codcadastro, id_cobrancas)
                        VALUES
                        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                          status = VALUES(status),
                          amount = VALUES(amount),
                          payment_date = VALUES(payment_date),
                          payer_cpf_cnpj = VALUES(payer_cpf_cnpj),
                          payer_name = VALUES(payer_name),
                          emv = VALUES(emv),
                          created_at = VALUES(created_at),
                          json_completo = VALUES(json_completo),
                          codigoparasistema = VALUES(codigoparasistema),
                          codcadastro = VALUES(codcadastro),
                          id_cobrancas = VALUES(id_cobrancas)
                    """, (
                        pix_id, surrogate_key, status, amount, payment_date, payer_cpf_cnpj, payer_name,
                        emv, created_at,
                        json.dumps(item, ensure_ascii=False),
                        codigoparasistema, codcadastro, id_cobrancas
                    ))

                    # 5) opcional: atualiza status na tabela de cobranças
                    if id_cobrancas:
                        # se veio PAID, marca como PAID
                        if status in ("PAID", "PIX_PAID", "SUCCESSFUL", "PIX_SUCCESSFUL"):
                            cursor.execute("""
                                UPDATE pix_cobrancas_geradas
                                SET status = 'PAGO', updated_at = NOW()
                                WHERE id_cobrancas = %s
                            """, (id_cobrancas,))

                    salvos += 1

                except Exception as e:
                    erros.append(str(e))
                    ignorados += 1

        conn.commit()
        return jsonify({"ok": True, "salvos": salvos, "ignorados": ignorados, "erros": erros[:3]}), 200

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)