from app import app
import json

with app.test_client() as client:
    resp = client.get('/api/structuring-operations')
    print("STATUS:", resp.status_code)
    try:
        print("JSON length:", len(resp.get_json()))
    except:
        print("RAW:", resp.data.decode('utf-8'))
