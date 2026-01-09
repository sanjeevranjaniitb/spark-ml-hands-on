#!/usr/bin/env python3

from flask import Flask, render_template, request, jsonify
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.preprocessing import LabelEncoder
import pickle
import os

app = Flask(__name__)

class MLPredictor:
    def __init__(self):
        self.churn_model = None
        self.house_model = None
        self.gender_encoder = None
        self.models_dir = "models/python"
        self.ensure_models_dir()
        self.load_or_train_models()
    
    def ensure_models_dir(self):
        os.makedirs(self.models_dir, exist_ok=True)
    
    def load_or_train_models(self):
        churn_model_path = f"{self.models_dir}/churn_model.pkl"
        house_model_path = f"{self.models_dir}/house_model.pkl"
        encoder_path = f"{self.models_dir}/gender_encoder.pkl"
        
        if (os.path.exists(churn_model_path) and 
            os.path.exists(house_model_path) and 
            os.path.exists(encoder_path)):
            print("Loading saved models...")
            self.load_models()
        else:
            print("Training new models...")
            self.train_and_save_models()
    
    def train_and_save_models(self):
        churn_df = pd.read_csv('data/churn.csv')
        housing_df = pd.read_csv('data/housing.csv')
        
        self.gender_encoder = LabelEncoder()
        churn_df['gender_encoded'] = self.gender_encoder.fit_transform(churn_df['gender'])
        
        X_churn = churn_df[['age', 'salary', 'gender_encoded']]
        y_churn = churn_df['churn']
        
        self.churn_model = LogisticRegression(random_state=42)
        self.churn_model.fit(X_churn, y_churn)
        
        X_house = housing_df[['rooms', 'area', 'age']]
        y_house = housing_df['price']
        
        self.house_model = LinearRegression()
        self.house_model.fit(X_house, y_house)
        
        with open(f"{self.models_dir}/churn_model.pkl", 'wb') as f:
            pickle.dump(self.churn_model, f)
        
        with open(f"{self.models_dir}/house_model.pkl", 'wb') as f:
            pickle.dump(self.house_model, f)
        
        with open(f"{self.models_dir}/gender_encoder.pkl", 'wb') as f:
            pickle.dump(self.gender_encoder, f)
        
        print("Models trained and saved successfully")
        print(f"Churn model accuracy: {self.churn_model.score(X_churn, y_churn):.3f}")
        print(f"House model R²: {self.house_model.score(X_house, y_house):.3f}")
    
    def load_models(self):
        with open(f"{self.models_dir}/churn_model.pkl", 'rb') as f:
            self.churn_model = pickle.load(f)
        
        with open(f"{self.models_dir}/house_model.pkl", 'rb') as f:
            self.house_model = pickle.load(f)
        
        with open(f"{self.models_dir}/gender_encoder.pkl", 'rb') as f:
            self.gender_encoder = pickle.load(f)
        
        print("Models loaded from disk successfully")

predictor = MLPredictor()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/predict/churn', methods=['POST'])
def predict_churn():
    data = request.json
    age = data['age']
    salary = data['salary']
    gender = data['gender']
    
    gender_encoded = predictor.gender_encoder.transform([gender])[0]
    
    features = np.array([[age, salary, gender_encoded]])
    prediction = predictor.churn_model.predict(features)[0]
    probability = predictor.churn_model.predict_proba(features)[0]
    
    return jsonify({
        'prediction': int(prediction),
        'probability': probability.tolist(),
        'churn_probability': float(probability[1])
    })

@app.route('/predict/house', methods=['POST'])
def predict_house():
    data = request.json
    rooms = data['rooms']
    area = data['area']
    age = data['age']
    
    features = np.array([[rooms, area, age]])
    prediction = predictor.house_model.predict(features)[0]
    
    return jsonify({
        'prediction': float(prediction),
        'price_per_sqft': float(prediction / area)
    })

@app.route('/stats')
def get_stats():
    churn_df = pd.read_csv('data/churn.csv')
    housing_df = pd.read_csv('data/housing.csv')
    ratings_df = pd.read_csv('data/ratings.csv')
    
    models_status = {
        'churn_model': os.path.exists(f"{predictor.models_dir}/churn_model.pkl"),
        'house_model': os.path.exists(f"{predictor.models_dir}/house_model.pkl"),
        'gender_encoder': os.path.exists(f"{predictor.models_dir}/gender_encoder.pkl")
    }
    
    return jsonify({
        'datasets': {
            'churn': {
                'records': len(churn_df),
                'churn_rate': float(churn_df['churn'].mean()),
                'avg_age': float(churn_df['age'].mean()),
                'avg_salary': float(churn_df['salary'].mean())
            },
            'housing': {
                'records': len(housing_df),
                'avg_price': float(housing_df['price'].mean()),
                'avg_rooms': float(housing_df['rooms'].mean()),
                'avg_area': float(housing_df['area'].mean())
            },
            'ratings': {
                'records': len(ratings_df),
                'unique_users': int(ratings_df['userId'].nunique()),
                'unique_movies': int(ratings_df['movieId'].nunique()),
                'avg_rating': float(ratings_df['rating'].mean())
            }
        },
        'models': models_status
    })

@app.route('/models/info')
def models_info():
    model_files = []
    if os.path.exists(predictor.models_dir):
        for file in os.listdir(predictor.models_dir):
            if file.endswith('.pkl'):
                file_path = os.path.join(predictor.models_dir, file)
                file_size = os.path.getsize(file_path)
                model_files.append({
                    'name': file,
                    'size_kb': round(file_size / 1024, 2),
                    'path': file_path
                })
    
    return jsonify({
        'models_directory': predictor.models_dir,
        'model_files': model_files,
        'total_models': len(model_files)
    })

if __name__ == '__main__':
    print("Starting Spark ML Demo Web UI with Model Persistence")
    print(f"Models saved in: {predictor.models_dir}")
    print("Visit http://localhost:5000 to access the interface")
    app.run(debug=True, host='0.0.0.0', port=5000)