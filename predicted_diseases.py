from elasticsearch import Elasticsearch

# Elasticsearch settings
es = Elasticsearch(
    ['http://localhost:9200'],
    http_auth=('elastic', 'Db2Otq5erlqH9jIQwzNh')  # Replace with your actual username and password
)

# Define the index mapping
index_mapping = {
    "mappings": {
        "properties": {
            "patient_id": {"type": "keyword"},
            "timestamp": {"type": "date"},
            "predicted_disease": {"type": "keyword"},
            "input_data": {
                "properties": {
                    "symptoms": {"type": "keyword"}
                }
            }
        }
    }
}

# Create the index
index_name = "predicted_diseases_in"
res = es.indices.create(index=index_name, body=index_mapping)

# Check the response
if res["acknowledged"]:
    print(f"Index '{index_name}' created successfully.")
else:
    print(f"Failed to create index '{index_name}'.")
