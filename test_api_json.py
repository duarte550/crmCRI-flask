from app import app

with app.test_client() as client:
    r = client.get('/api/structuring-operations')
    if r.status_code != 200:
        print("Backend error:", r.data.decode('utf-8'))
    else:
        data = r.get_json()
        print(f"Success! Got {len(data)} operations")
