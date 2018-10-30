from flask import Flask, request, send_from_directory, redirect, jsonify
from flask_cors import CORS
import os
from analysis import analyze

# set the project root directory as the static folder, you can set others.
app = Flask(__name__)
CORS(app)

@app.route('/')
def index():
    return redirect('/index.html')

@app.route('/api/parse', methods=['POST'])
def api():
    data = request.get_json()
    plan, query = analyze(data['plan'], data['query'])
    return jsonify({
        'plan': plan,
        'query': query
    })

@app.route('/<path:path>')
def root(path):
    print(path)
    return send_from_directory('static', path)

if __name__ == "__main__":
    app.run()
