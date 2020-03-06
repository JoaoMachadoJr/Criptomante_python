import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
from sklearn.base import TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn import metrics
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
import  en_core_web_sm, en_core_web_lg





# Custom transformer using spaCy
class predictors(TransformerMixin):
    nlp_large = None

    def transform(self, X, **transform_params):
        return X

    def fit(self, X, y=None, **fit_params):
        return self

    def get_params(self, deep=True):
        return {}

# Basic function to clean the text
def clean_text(text):
    # Removing spaces and converting text into lowercase
    return text.strip().lower()

def realizar_teste(dataframe):
    print("Criando vetor")
    tfidf_vector = TfidfVectorizer(tokenizer = tokenizer)
    bow_vector = CountVectorizer(tokenizer = tokenizer)

    print("Criando eixos")
    X = dataframe['texto'] # the features we want to analyze
    ylabels = dataframe['tendencia'] # the labels, or answers, we want to test against
    X_train, X_test, y_train, y_test = train_test_split(X, ylabels, test_size=0.1)

    classifier = LogisticRegression(max_iter=999999999, n_jobs=6)

    print("Criando Pipe")
    # Create pipeline using Bag of Words
    pipe = Pipeline([('vectorizer', tfidf_vector),
                    ('classifier', classifier)],
                    verbose=True)

    print("Realizando FIT")
    # model generation
    pipe.fit(X_train,y_train)

    print("Realizando previsao")
    # Predicting with a test dataset
    predicted = pipe.predict(X_test)

    # Model Accuracy
    print("Logistic Regression Accuracy:",metrics.accuracy_score(y_test, predicted))
    print("Logistic Regression Precision:",metrics.precision_score(y_test, predicted))
    print("Logistic Regression Recall:",metrics.recall_score(y_test, predicted))

def tokenizer(sentence):
    return sentence.split(" ")
    

    

    