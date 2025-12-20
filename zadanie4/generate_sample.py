import csv
from datetime import datetime

with open('local_data/sample-small.csv','w',newline='',encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["event_time","event_type","product_id","category_id","category_code","brand","price","user_id","user_session"])
    for i in range(1000):
        writer.writerow([datetime.utcnow().isoformat()+"Z","view",i,1234,"cat.sub","brand",10.0+i%5, 1000+i, f"sess-{i}"])

print('sample created')