import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
from sklearn.base import TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn import metrics
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import LinearSVC
from sklearn.model_selection import train_test_split
import  en_core_web_sm, en_core_web_lg
from sklearn.metrics import confusion_matrix
from sklearn.naive_bayes import MultinomialNB


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


def construir_modelo(dataframe):
    #Classificador de Regressão Logística. Com método SAGA para solucionar o problema de minimização
    classificador = LogisticRegression(max_iter=999999999, n_jobs=6,solver='saga')

    #Tokenizador simples. Pois o texto já estava pré-tratado    
    vetorizador = TfidfVectorizer(tokenizer = tokenizer, ngram_range=(1,1))

    classifier = classificador
    tfidf_vector = vetorizador

    X = dataframe['texto'] # Eixo X, indica o texto de entrada. Sendo as frases lematizadas
    ylabels = dataframe['tendencia'] # Eixo Y, indica o a classificacao positiva ou negativa de cada frase.

    #Dividimos os dados de entrada. Sendo 90% para treinamento. E 10% para testar a qualidade do treinamento
    X_train, X_test, y_train, y_test = train_test_split(X, ylabels, test_size=0.1) 

    # Criamos um Pipeline, que indica a sequência em que as coisas são feitas
    pipe = Pipeline([('vectorizer', tfidf_vector),
                    ('classifier', classifier)],
                    verbose=True)

    #Realizando a previsão.
    pipe.fit(X_train,y_train)
    predicted = pipe.predict(X_test)

    #Preparando saida    
    saida = dict()
    matriz = confusion_matrix(y_test,predicted )
    saida["TP"] = matriz[1][1] #Verdadeiros Positivos
    saida["FP"] = matriz[0][1] #Falsos Positivos
    saida["TN"] = matriz[0][0] #Verdadeiros Negativos
    saida["FN"] = matriz[1][0] #Falsos negativos



    saida["modelo"] = pipe
    saida["Accuracy"]= metrics.accuracy_score(y_test, predicted)
    saida["Precision"] = metrics.precision_score(y_test, predicted)
    saida["Recall"] = metrics.recall_score(y_test, predicted)
    saida["X_train"] = X_train
    saida["X_test"] = X_test
    saida["y_train"] = y_train
    saida["y_test"] = y_test
    
    


    return saida



def comparar_modelos(dataframe):
    classificadores = dict()
    classificadores["LogisticRegression"] = LogisticRegression(max_iter=999999999, n_jobs=6)
    classificadores["LogisticRegression, newton-cg"] = LogisticRegression(max_iter=999999999, solver='newton-cg')
    classificadores["LogisticRegression, liblinear"] = LogisticRegression(max_iter=999999999, n_jobs=6,solver='liblinear')
    classificadores["LogisticRegression, sag"] = LogisticRegression(max_iter=999999999, n_jobs=6,solver='sag')
    classificadores["LogisticRegression, saga"] = LogisticRegression(max_iter=999999999, n_jobs=6,solver='saga')
    classificadores["RandomForestClassifier"] = RandomForestClassifier(n_estimators=200, max_depth=3, random_state=0, n_jobs=6)
    classificadores["LinearSVC"] = LinearSVC()
    classificadores["MultinomialNB"] = MultinomialNB()


    vetorizadores = dict()
    vetorizadores["tfidf_vector 11"] = TfidfVectorizer(tokenizer = tokenizer, ngram_range=(1,1))
    vetorizadores["tfidf_vector 33"] = TfidfVectorizer(tokenizer = tokenizer, ngram_range=(3,3))
    vetorizadores["tfidf_vector 55"] = TfidfVectorizer(tokenizer = tokenizer, ngram_range=(5,5))

    for cla in classificadores.keys():
        for vet in vetorizadores.keys():
            classifier = classificadores[cla]
            tfidf_vector = vetorizadores[vet]


            X = dataframe['texto'] # the features we want to analyze
            ylabels = dataframe['tendencia'] # the labels, or answers, we want to test against
            X_train, X_test, y_train, y_test = train_test_split(X, ylabels, test_size=0.1)


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
            print("Precisao "+cla+"   "+vet+"   =",metrics.precision_score(y_test, predicted))

    saida = dict()
    saida["modelo"] = pipe
    saida["Accuracy"]= metrics.accuracy_score(y_test, predicted)
    saida["Precision"] = metrics.precision_score(y_test, predicted)
    saida["Recall"] = metrics.recall_score(y_test, predicted)
    saida["X_train"] = X_train
    saida["X_test"] = X_test
    saida["y_train"] = y_train
    saida["y_test"] = y_test
    matriz = confusion_matrix(y_test,predicted )
    saida["TP"] = matriz[1][1]
    saida["FP"] = matriz[0][1]
    saida["TN"] = matriz[0][0]
    saida["FN"] = matriz[1][0]


    return saida
     
def tokenizer(sentence):
    return sentence.split(" ")




    

    

    