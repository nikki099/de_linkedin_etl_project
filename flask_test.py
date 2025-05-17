from flask import Flask, session
from flask_session import Session

app = Flask(__name__)
app.config['SECRET_KEY'] = 'test_secret_key'   # 用于加密session
app.config['SESSION_TYPE'] = 'filesystem'     # 使用文件系统存储session

Session(app)  # 初始化 Flask-Session

@app.route('/')
def index():
    session['counter'] = session.get('counter', 0) + 1
    return f"Session works! You have visited this page {session['counter']} times."

if __name__ == '__main__':
    app.run(debug=True)
