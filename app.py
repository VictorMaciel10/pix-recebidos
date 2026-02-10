from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "pix-recebidos"
    })

@app.route("/webhook/pix", methods=["POST"])
def webhook_pix():
    dados = request.get_json(silent=True)
    print("PIX recebido:")
    print(dados)
    return jsonify({"ok": True}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)