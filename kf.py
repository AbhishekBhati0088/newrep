import pickle
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import warnings
import json
import time
from datetime import datetime
warnings.filterwarnings("ignore")


# Counter for patient ID
patient_counter = 1

l1 = ['back_pain', 'constipation', 'abdominal_pain', 'diarrhoea',
       'mild_fever', 'yellow_urine', 'yellowing_of_eyes',
       'acute_liver_failure', 'fluid_overload', 'swelling_of_stomach',
       'swelled_lymph_nodes', 'malaise', 'blurred_and_distorted_vision',
       'phlegm', 'throat_irritation', 'redness_of_eyes', 'sinus_pressure',
       'runny_nose', 'congestion', 'chest_pain', 'weakness_in_limbs',
       'fast_heart_rate', 'pain_during_bowel_movements', 'pain_in_anal_region',
       'bloody_stool', 'irritation_in_anus', 'neck_pain', 'dizziness',
       'cramps', 'bruising', 'obesity', 'swollen_legs',
       'swollen_blood_vessels', 'puffy_face_and_eyes', 'enlarged_thyroid',
       'brittle_nails', 'swollen_extremeties', 'excessive_hunger',
       'extra_marital_contacts', 'drying_and_tingling_lips', 'slurred_speech',
       'knee_pain', 'hip_joint_pain', 'muscle_weakness', 'stiff_neck',
       'swelling_joints', 'movement_stiffness', 'spinning_movements',
       'loss_of_balance', 'unsteadiness', 'weakness_of_one_body_side',
       'loss_of_smell', 'bladder_discomfort', 'foul_smell_of urine',
       'continuous_feel_of_urine', 'passage_of_gases', 'internal_itching',
       'toxic_look_(typhos)', 'depression', 'irritability', 'muscle_pain',
       'altered_sensorium', 'red_spots_over_body', 'belly_pain',
       'abnormal_menstruation', 'dischromic _patches', 'watering_from_eyes',
       'increased_appetite', 'polyuria', 'family_history', 'mucoid_sputum',
       'rusty_sputum', 'lack_of_concentration', 'visual_disturbances',
       'receiving_blood_transfusion', 'receiving_unsterile_injections', 'coma',
       'stomach_bleeding', 'distention_of_abdomen',
       'history_of_alcohol_consumption', 'fluid_overload', 'blood_in_sputum',
       'prominent_veins_on_calf', 'palpitations', 'painful_walking',
       'pus_filled_pimples', 'blackheads', 'scurring', 'skin_peeling',
       'silver_like_dusting', 'small_dents_in_nails', 'inflammatory_nails',
       'blister', 'red_sore_around_nose', 'yellow_crust_ooze']

disease=['Fungal infection', 'Allergy', 'GERD', 'Chronic cholestasis',
       'Drug Reaction', 'Peptic ulcer diseae', 'AIDS', 'Diabetes ',
       'Gastroenteritis', 'Bronchial Asthma', 'Hypertension ', 'Migraine',
       'Cervical spondylosis', 'Paralysis (brain hemorrhage)', 'Jaundice',
       'Malaria', 'Chicken pox', 'Dengue', 'Typhoid', 'hepatitis A',
       'Hepatitis B', 'Hepatitis C', 'Hepatitis D', 'Hepatitis E',
       'Alcoholic hepatitis', 'Tuberculosis', 'Common Cold', 'Pneumonia',
       'Dimorphic hemmorhoids(piles)', 'Heart attack', 'Varicose veins',
       'Hypothyroidism', 'Hyperthyroidism', 'Hypoglycemia',
       'Osteoarthristis', 'Arthritis',
       '(vertigo) Paroymsal  Positional Vertigo', 'Acne',
       'Urinary tract infection', 'Psoriasis', 'Impetigo']



# Load the trained model from the pickle file
with open('C:/Users/I4628/Healthcare_pro/healthcare/HealthModel.pkl', 'rb') as file:
    model = pickle.load(file)

# Function to generate a unique patient ID
def generate_patient_id():
    global patient_counter
    patient_id = f"PATIENT_{patient_counter}"
    patient_counter += 1
    return patient_id


# Function to convert timestamp to a proper date format
def format_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp).isoformat()

# Function to make predictions
def make_prediction(symptoms):
    l2 = [0] * len(l1)
    for i in range(len(l1)):
        if l1[i] in symptoms:
            l2[i] = 1
    input_test = [l2]
    prediction = model.predict(input_test)
    predicted_disease = disease[prediction[0]]
    return predicted_disease

# Kafka consumer settings
bootstrap_servers = 'localhost:9092'
topic = 'kafka-healthcare-ntopic'

# Elasticsearch settings
es = Elasticsearch(
    ['http://localhost:9200'],
    http_auth=('elastic', 'Db2Otq5erlqH9jIQwzNh')  # Replace with your actual username and password
)

# Create Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

# Consume messages from Kafka topic
for message in consumer:
    # Decode the received message
    json_data = json.loads(message.value.decode('utf-8'))

    # Check if the json_data is a list instead of a dictionary
    if isinstance(json_data, list):
        for item in json_data:
            # Extract symptoms from the JSON data
            symptoms = [item['data']['symptom1'], item['data']['symptom2'], item['data']['symptom3'], item['data']['symptom4']]
            symptoms = [symptom for symptom in symptoms if symptom != 'NA']

            # Make prediction using the symptoms
            predicted_disease = make_prediction(symptoms)

            # Generate patient ID and timestamp
            patient_id = generate_patient_id()
            timestamp = format_timestamp(time.time())

            # Print the predicted disease
            print('Patient ID:', patient_id)
            print('Timestamp:', timestamp)
            print('Patient is having high chances of:', predicted_disease)

            # Create a dictionary with the desired key-value pairs
            data = {
                'patient_id': patient_id,
                'timestamp': timestamp,
                'predicted_disease': predicted_disease,
                'symptoms': symptoms
            }

            # Convert the dictionary to JSON format
            json_data = json.dumps(data)

            # Update the doc dictionary with patient ID, timestamp, and predicted disease
            #doc = {
                #'patient_id': patient_id,
                #'timestamp': timestamp,
                #'predicted_disease': predicted_disease,
                #'input_data': {
                    #'symptoms': json_data
               # }
            #}

            # Send data to Elasticsearch
            try:
                res = es.index(index='predicted_diseases_in', body=json_data)
                print('Data sent to Elasticsearch:', res)
            except Exception as e:
                print('Failed to send data to Elasticsearch:', e)



