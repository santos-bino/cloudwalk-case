from flask import Flask, request, jsonify
from monitoring import predict_status

app = Flask(__name__)

@app.route('/monitorar', methods=['POST'])
def chamar_funcao():
    time = request.json['time']
    count = request.json['count']
    resultado = predict_status(time, count)
    return jsonify({'resultado': resultado})

if __name__ == '__main__':
    app.run(debug=True)

