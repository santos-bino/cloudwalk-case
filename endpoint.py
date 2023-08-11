from flask import Flask, request, jsonify
from monitoring import predict_status
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

app = Flask(__name__)

@app.route('/monitorar', methods=['POST'])
def chamar_funcao():
    time = request.json['time']
    count = request.json['count']
    resultado = predict_status(time, count)
    return jsonify({'resultado': resultado})


# Configurações do servidor SMTP e credenciais
smtp_server = 'smtp.gmail.com'
smtp_port = 587
smtp_username = 'santos.gabrielsilva83@gmail.com'
smtp_password = 'dfacpohgsfsijqnb'  # Lembre-se de usar a senha de aplicativo

@app.route('/enviar_email', methods=['POST'])
def enviar_email():
    data = request.json
    to_email = 'santos.gabriel.silva@hotmail.com'
    subject = 'ALERTA DE ANOMALIA!!'
    body = data.get('body')

    if not to_email or not subject or not body:
        return 'Dados incompletos', 400

    msg = MIMEMultipart()
    msg['From'] = smtp_username
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_username, to_email, msg.as_string())
        server.quit()
        return 'E-mail enviado com sucesso', 200
    except Exception as e:
        return f'Erro ao enviar o e-mail: {str(e)}', 500

if __name__ == '__main__':
    app.run(debug=True)
