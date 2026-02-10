import os
import json
import pymysql
from flask import Flask, request, jsonify
from dateutil import parser

app = Flask(__name__)

# =========================
# Helpers
# =========================
def iso_to_mysql_dt(s):
    """Converte ISO 8601 (ex: 2021-08-25T00:19:23.248Z) -> 'YYYY-MM-DD HH:MM:SS' (UTC)."""
    if not s:
        return None
    try:
        dt = parser.isoparse(str(s))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def conectar_banco():
    """Conexão MySQL via variáveis de ambiente (Railway Variables)."""
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


def norm_status(s):
    return str(s or "").strip().upper()


def is_paid_event(item: dict) -> bool:
    """
    Regra de "pago":
    - Se vier paymentDate preenchido, consideramos pago (bem confiável).
    - Se não vier, pode estar "ACTIVE" ou outro status -> ignora.
    """
    return bool(item.get("paymentDate"))


# =========================
# Rotas
# =========================
@app.route("/", methods=["GET"])
def health():
    return jsonify({"service": "pix-recebidos", "status": "ok"}), 200


@app.route("/webhook/pix-pago", methods=["POST"])
def webhook_pix_pago():
    """
    Recebe uma lista (ou objeto único) no formato:
    [
      {
        "id": "...",
        "surrogateKey": "...",
        "status": "PAID",
        "amount": 5.25,
        "paymentDate": "2021-08-25T00:19:23.248Z",
        "payerCpfCnpj": "...",
        "payerName": "...",
        "emv": "...",
        "createdAt": "2021-08-25T00:18:53.618Z"
      }
    ]

    Salva APENAS os pagos (paymentDate != null).
    Além disso, identifica a empresa/cadastro cruzando pix_id com pix_cobrancas_geradas
    e grava em pix_recebidos: codigoparasistema, codcadastro, id_cobrancas.
    Também atualiza a cobrança para status='PAID'.
    """
    conn = None
    try:
        payload = request.get_json(silent=True)
        if not payload:
            return jsonify({"ok": False, "error": "JSON vazio/inválido"}), 400

        itens = payload if isinstance(payload, list) else [payload]

        salvos = 0
        ignorados = 0
        sem_vinculo = 0  # pago, mas não achou pix_id na pix_cobrancas_geradas

        conn = conectar_banco()
        with conn.cursor() as cursor:
            for item in itens:
                if not isinstance(item, dict):
                    ignorados += 1
                    continue

                pix_id = str(item.get("id") or "").strip()
                if not pix_id:
                    ignorados += 1
                    continue

                # ✅ só grava pagos
                if not is_paid_event(item):
                    ignorados += 1
                    continue

                status = norm_status(item.get("status"))
                payment_date = iso_to_mysql_dt(item.get("paymentDate"))

                surrogate_key = item.get("surrogateKey")
                amount = item.get("amount")
                payer_cpf_cnpj = item.get("payerCpfCnpj")
                payer_name = item.get("payerName")
                emv = item.get("emv")
                created_at = iso_to_mysql_dt(item.get("createdAt"))

                # ✅ Busca cobrança gerada (pra descobrir empresa / cadastro / id_cobrancas)
                cursor.execute("""
                    SELECT
                      id_cobrancas,
                      codigoparasistema,
                      codcadastro
                    FROM develop_1_lic.pix_cobrancas_geradas
                    WHERE pix_id = %s
                    LIMIT 1
                """, (pix_id,))
                cobranca = cursor.fetchone() or {}

                codigoparasistema = cobranca.get("codigoparasistema")
                codcadastro = cobranca.get("codcadastro")
                id_cobrancas = cobranca.get("id_cobrancas")

                if not id_cobrancas:
                    # ainda assim salva o recebimento, mas marca como sem vínculo
                    sem_vinculo += 1

                # ✅ Salva recebimento já com empresa/cadastro/id_cobrancas
                cursor.execute("""
                    INSERT INTO develop_1_lic.pix_recebidos
                      (pix_id, surrogate_key, status, amount, payment_date,
                       payer_cpf_cnpj, payer_name, emv, created_at,
                       codigoparasistema, codcadastro, id_cobrancas, json_completo)
                    VALUES
                      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      surrogate_key = VALUES(surrogate_key),
                      status        = VALUES(status),
                      amount        = VALUES(amount),
                      payment_date  = VALUES(payment_date),
                      payer_cpf_cnpj= VALUES(payer_cpf_cnpj),
                      payer_name    = VALUES(payer_name),
                      emv           = VALUES(emv),
                      created_at    = VALUES(created_at),
                      codigoparasistema = VALUES(codigoparasistema),
                      codcadastro   = VALUES(codcadastro),
                      id_cobrancas  = VALUES(id_cobrancas),
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
                    codigoparasistema,
                    codcadastro,
                    id_cobrancas,
                    json.dumps(item, ensure_ascii=False)
                ))

                # ✅ Atualiza status da cobrança como PAID (se existir vínculo)
                if id_cobrancas:
                    cursor.execute("""
                        UPDATE develop_1_lic.pix_cobrancas_geradas
                        SET status = 'PAID',
                            updated_at = NOW()
                        WHERE id_cobrancas = %s
                        LIMIT 1
                    """, (id_cobrancas,))

                salvos += 1

            conn.commit()

        return jsonify({
            "ok": True,
            "salvos": salvos,
            "ignorados": ignorados,
            "pagos_sem_vinculo": sem_vinculo
        }), 200

    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        print("Erro webhook:", e)
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    # Railway define PORT automaticamente
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)